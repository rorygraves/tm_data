package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.area.perf.{HistoricAreaPerfTable, TMAreaDataDownloader}
import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, TMDataClubInfoTable}
import com.github.rorygraves.tm_data.data.club.perf.{HistoricClubPerfGenerator, HistoricClubPerfTable}
import com.github.rorygraves.tm_data.data.district.{DistrictImportResult, FailedDistrictImportResult, SuccessfulDistrictImportResult}
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, TMDataDistrictSummaryHistoricalTable}
import com.github.rorygraves.tm_data.data.division.{HistoricDivisionPerfTable, TMDivisionDataDownloader}
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
      progStartYearMonth: YearMonth
  ): DistrictImportResult = {

    val clubCount = clubInfoGenerator.generateClubData(districtId, false)

    val latestMonthClubRows = historicClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      progStartYearMonth
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
        logger.error("Exception during main import", e)
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

    val historicDivisionPerfTableDef = new HistoricDivisionPerfTable(dbRunner)
    val divisionDownloader           = new TMDivisionDataDownloader(historicDivisionPerfTableDef)

    historicAreaPerfTableDef.createIfNotExists()
    historicDivisionPerfTableDef.createIfNotExists()
    logger.info("Ensuring club info table exists")
    clubInfoTable.createIfNotExists()
    logger.info("Ensuring historic club performance table exists")
    historicClubPerfTable.createIfNotExists()
    logger.info("Ensuring district summary historical table exists")
    districtSummaryHistoricalTable.createIfNotExists()

    logger.info("Generating historical overview data")

    districtSummaryHistoricalGenerator.generateHistoricalOverviewData(cacheFolder, startYear)

    logger.info("Fetching district Ids")
    val allDistrictIds = districtSummaryHistoricalTable.allDistrictIds().sorted

    logger.info("Found " + allDistrictIds.size + " districts")

    logger.info("Fetching latest month end dates by district")
    val latestMonthEndDatesByDistrict = historicClubPerfTable.latestDistrictMonthDates()

    logger.info("Found " + latestMonthEndDatesByDistrict.size + " districts")

    val parallelismLevel = 1

    val districtCount = allDistrictIds.size

    val completedCount = new AtomicInteger(0)

    logger.info(s"Running per district ($districtCount) imports in parallel - parallelism level $parallelismLevel")
    val futures = allDistrictIds
      .grouped(Math.max(allDistrictIds.size / parallelismLevel, 1))
      .map { group =>
        Future {
          group.map { districtId =>
            val latest = latestMonthEndDatesByDistrict.get(districtId)
            val first  = districtSummaryHistoricalTable.firstOverviewDate(districtId)

            val progStartYearMonth = (first, latest) match {
              case (None, None) =>
                throw new IllegalStateException("No start date found for district " + districtId)
              case (Some(first), None) =>
                first
              case (_, Some(latest)) =>
                latest
            }

            try {

              logger.info("Generation area data for district " + districtId + "--------------------------------------")
              areaDownloader.generateHistoricalAreaData(cacheFolder, districtId, startYear)
              logger.info("Generation division data for district " + districtId + "----------------------------------")
              divisionDownloader.generateHistoricalDivData(cacheFolder, districtId, startYear)
              logger.info("Generating club info for district " + districtId + "--------------------------------------")
              val res = generateDistrictData(
                clubInfoGenerator,
                historicClubPerfGenerator,
                districtId,
                progStartYearMonth
              )
              val curCompleted = completedCount.incrementAndGet()
              logger.info(s"Completed district $districtId  ($curCompleted/$districtCount)")
              Thread.sleep(30000) // sleep for 30 seconds between district imports to avoid rate limiting
              res
            } catch {
              case e: Exception =>
                logger.error(s"Failed to load district data $districtId", e)
                FailedDistrictImportResult(districtId, e.toString)
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
