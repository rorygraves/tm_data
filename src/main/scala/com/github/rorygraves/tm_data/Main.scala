package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.area.perf.{HistoricAreaPerfTable, TMAreaDataDownloader}
import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, TMDataClubInfoTable}
import com.github.rorygraves.tm_data.data.club.perf.{HistoricClubPerfGenerator, HistoricClubPerfTable}
import com.github.rorygraves.tm_data.data.district.{
  DistrictImportResult,
  FailedDistrictImportResult,
  SuccessfulDistrictImportResult
}
import com.github.rorygraves.tm_data.data.district.historical.{
  DistrictSummaryHistoricalGenerator,
  TMDataDistrictSummaryHistoricalTable
}
import com.github.rorygraves.tm_data.data.division.{HistoricDivisionPerfTableDef, TMDivisionDataDownloader}
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

    val clubCount = clubInfoGenerator.generateClubData(districtId, false)

    val latestMonthClubRows = historicClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      progStartYear,
      progStartMonth
    )

    SuccessfulDistrictImportResult(districtId, clubCount, latestMonthClubRows)
  }

  def main(args: Array[String]): Unit = {

    val db       = Database.forConfig("tm_data")
    val dbRunner = new FixedDBRunner(db)
    try {
      runMainImport(dbRunner)

    } catch {
      case e: Exception =>
        println("Exception during main import")
        e.printStackTrace()
        System.exit(1)

    } finally db.close

  }
  def runMainImport(dbRunner: DBRunner): Unit = {

    val startYear = 2012

    val clubInfoTable                      = new TMDataClubInfoTable(dbRunner)
    val clubInfoGenerator                  = new ClubInfoGenerator(clubInfoTable)
    val districtSummaryHistoricalTable     = new TMDataDistrictSummaryHistoricalTable(dbRunner)
    val districtSummaryHistoricalGenerator = new DistrictSummaryHistoricalGenerator(districtSummaryHistoricalTable)
    val historicClubPerfTable              = new HistoricClubPerfTable(dbRunner)
    val historicClubPerfGenerator =
      new HistoricClubPerfGenerator(districtSummaryHistoricalTable, historicClubPerfTable)

    val historicAreaPerfTableDef = new HistoricAreaPerfTable(dbRunner)
    val areaDownloader           = new TMAreaDataDownloader(historicAreaPerfTableDef)

    val historicDivisionPerfTableDef = new HistoricDivisionPerfTableDef(dbRunner)
    val divisionDownloader           = new TMDivisionDataDownloader(historicDivisionPerfTableDef)

    historicAreaPerfTableDef.createIfNotExists()
    historicDivisionPerfTableDef.createIfNotExists()
    println("Ensuring club info table exists")
    clubInfoTable.createIfNotExists()
    println("Ensuring historic club performance table exists")
    historicClubPerfTable.createIfNotExists()
    println("Ensuring district summary historical table exists")
    districtSummaryHistoricalTable.createIfNotExists()

    logger.info("Generating historical overview data")

    districtSummaryHistoricalGenerator.generateHistoricalOverviewData(cacheFolder, startYear)

    logger.info("Fetching district Ids")
    val allDistrictIds = "91" :: districtSummaryHistoricalTable.allDistrictIds().sorted.filter(_ != "91")

    logger.info("Found " + allDistrictIds.size + " districts")

    logger.info("Fetching latest month end dates by district")
    val latestMonthEndDatesByDistrict = historicClubPerfTable.latestDistrictMonthDates()

    logger.info("Found " + latestMonthEndDatesByDistrict.size + " districts")

    val parallelismLevel = 2

    val districtCount = latestMonthEndDatesByDistrict.size

    val completedCount = new AtomicInteger(0)

    logger.info(s"Running per district ($districtCount) imports in parallel - parallelism level $parallelismLevel")
    val futures = allDistrictIds
      .grouped(Math.max(allDistrictIds.size / parallelismLevel, 1))
      .map { group =>
        Future {
          group.map { districtId =>
            val (progStartYear, progStartMonth) = latestMonthEndDatesByDistrict.getOrElse(districtId, (startYear, 1))
            try {

              logger.info("Generation area data for district " + districtId + "--------------------------------------")
              areaDownloader.generateHistoricalAreaData(cacheFolder, districtId, startYear, 1)
              logger.info("Generation division data for district " + districtId + "----------------------------------")
              divisionDownloader.generateHistoricalDivData(cacheFolder, districtId, startYear, 1)

              logger.info("Generating club info for district " + districtId + "--------------------------------------")
              val res = generateDistrictData(
                clubInfoGenerator,
                historicClubPerfGenerator,
                districtId,
                progStartYear,
                progStartMonth
              )
              val curCompleted = completedCount.incrementAndGet()
              logger.info(s"Completed district $districtId  ($curCompleted/$districtCount)")
              res
            } catch {
              case e: Exception =>
                logger.error(s"Failed to load district data $districtId", e)
//                System.exit(1)
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
