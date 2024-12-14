package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, TMDataClubInfoTable}
import com.github.rorygraves.tm_data.data.club.perf.historical.HistoricClubPerfGenerator
import com.github.rorygraves.tm_data.data.club.perf.historical.data.HistoricClubPerfTableDef
import com.github.rorygraves.tm_data.data.district.{DistrictImportResult, FailedDistrictImportResult, SuccessfulDistrictImportResult}
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, TMDataDistrictSummaryHistoricalTable}
import com.github.rorygraves.tm_data.util.{DBRunner, FixedDBRunner}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile.api._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object Main {

  val logger: Logger      = LoggerFactory.getLogger(getClass)
  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  def generateDistrictData(
      clubInfoGenerator: ClubInfoGenerator,
      historicClubPerfGenerator: HistoricClubPerfGenerator,
      districtId: String,
      progStartYear: Int,
      progStartMonth: Int
  ): DistrictImportResult = {

    val clubCount = clubInfoGenerator.generateClubData(districtId)

    val latestMonthClubRows = historicClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      progStartYear,
      progStartMonth
    )

    SuccessfulDistrictImportResult(districtId, clubCount, latestMonthClubRows)
  }

  def main(args: Array[String]): Unit = {

//    val config: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig[JdbcProfile]("tm_data")
    val db       = Database.forConfig("tm_data")
    val dbRunner = new FixedDBRunner(db)
    try {
      runMainImport(dbRunner)

//      val districtId = "91"
//      val clubs      = ClubInfoTableDef.allClubInfo(districtId, db)
//      println("Clubs.size = " + clubs.size)
//      println("Generating club info for district " + districtId)
//      ClubInfoGenerator.generateClubData(districtId, db)

    } catch {
      case e: Exception =>
        println("Exception during main import")
        e.printStackTrace()
        System.exit(1)

    } finally db.close

  }
  def runMainImport(dbRunner: DBRunner): Unit = {

    val clubInfoTableDef                   = new TMDataClubInfoTable(dbRunner)
    val clubInfoGenerator                  = new ClubInfoGenerator(clubInfoTableDef)
    val districtSummaryHistoricalTableDef  = new TMDataDistrictSummaryHistoricalTable(dbRunner)
    val districtSummaryHistoricalGenerator = new DistrictSummaryHistoricalGenerator(districtSummaryHistoricalTableDef)
    val historicClubPerfTableDef           = new HistoricClubPerfTableDef(dbRunner)
    val historicClubPerfGenerator          = new HistoricClubPerfGenerator(districtSummaryHistoricalTableDef, historicClubPerfTableDef)
//    println("Ensuring club info table exists")
//    ClubInfoTableDef.createIfNotExists(dbRunner)
//    println("Ensuring historic club performance table exists")
//    HistoricClubPerfTableDef.createIfNotExists(dbRunner)
//    println("Ensuring district summary historical table exists")
//    DistrictSummaryHistoricalTableDef.createIfNotExists(dbRunner)

    logger.info("Generating historical overview data")

    districtSummaryHistoricalGenerator.generateHistoricalOverviewData(cacheFolder)

    logger.info("Fetching district Ids")
    val allDistrictIds = districtSummaryHistoricalTableDef.allDistrictIds().sorted
    allDistrictIds.foreach { districtId =>
      println("Generating club info for district " + districtId)
    }
    logger.info("Found " + allDistrictIds.size + " districts")

    logger.info("Fetching latest month end dates by district")
    val latestMonthEndDatesByDistrict = historicClubPerfTableDef.latestDistrictMonthDates().toList

    val clubCount = latestMonthEndDatesByDistrict.size
    logger.info("Found " + latestMonthEndDatesByDistrict.size + " districts")

    val parallelismLevel = 4

    if (allDistrictIds.size != latestMonthEndDatesByDistrict.size)
      throw new IllegalStateException("BANG - mismatch - new District?!")

    val districtCount = latestMonthEndDatesByDistrict.size

    val completedCount = new AtomicInteger(0)

    logger.info(s"Running per district ($districtCount) imports in parallel - parallelism level $parallelismLevel")
    val futures = latestMonthEndDatesByDistrict
      .grouped(clubCount / parallelismLevel)
      .map { group =>
        Future {
          group.map { case (districtId, (progStartYear, progStartMonth)) =>
            try {
              logger.info("Generating club info for district " + districtId + "--------------------------------------")
              val res = generateDistrictData(clubInfoGenerator, historicClubPerfGenerator, districtId, progStartYear, progStartMonth)
              val curCompleted = completedCount.incrementAndGet()
              logger.info(s"Completed district $districtId  ($curCompleted/$districtCount)")
              res
            } catch {
              case e: Exception =>
                logger.error(s"Failed to load district data $districtId", e)
                System.exit(1)
            }
          }
        }
      }
      .toList

    val results = Await.result(Future.sequence(futures), Duration.Inf).flatten
    // separate results into successful and failed
    val successful = results.collect { case s: SuccessfulDistrictImportResult => s }
    val failed     = results.collect { case f: FailedDistrictImportResult => f }

    logger.info("Successful imports:")
    successful.foreach { s =>
      logger.info(s.toString)
    }

    logger.info("Failed imports:")
    failed.foreach { f =>
      logger.error(f.toString)
    }

  }
}
