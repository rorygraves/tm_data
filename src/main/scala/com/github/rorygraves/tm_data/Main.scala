package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.area.perf.{HistoricAreaPerfTable, TMAreaDataDownloader}
import com.github.rorygraves.tm_data.data.club.info.{
  ClubInfoGenerator,
  ClubInfoWithHistory,
  TMDataClubInfoTable,
  TMDataClubInfoHistoricalTable
}
import com.github.rorygraves.tm_data.data.club.perf.{HistoricClubPerfGenerator, HistoricClubPerfTable}
import com.github.rorygraves.tm_data.data.district.{DistrictImportResult, FailedDistrictImportResult, SuccessfulDistrictImportResult}
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, TMDataDistrictSummaryHistoricalTable}
import com.github.rorygraves.tm_data.data.district.TMDataDistrictRegionTable
import com.github.rorygraves.tm_data.data.division.{HistoricDivisionPerfTable, TMDivisionDataDownloader}
import com.github.rorygraves.tm_data.util.{ConfigDBRunner, DBRunner}
import org.slf4j.{Logger, LoggerFactory}
import slick.basic.DatabaseConfig
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.JdbcProfile
import toasthost.utils.ToastmastersDateUtils

import scala.annotation.tailrec

object Main {

  val logger: Logger = LoggerFactory.getLogger("Main")

  private val RecentMonthsFlag          = "--recent-months"
  private val RecentMonthsFlagWithValue = "--recent-months="
  private val RecentMonthsEnvVar        = "TM_DATA_RECENT_MONTHS_THRESHOLD"
  private val DistrictsFlag             = "--districts"
  private val DistrictsFlagWithValue    = "--districts="

  def generateDistrictData(
      clubInfoGenerator: ClubInfoGenerator,
      historicClubPerfGenerator: HistoricClubPerfGenerator,
      districtId: String,
      progStartYearMonth: YearMonth
  ): DistrictImportResult = {

    val clubCount = clubInfoGenerator.generateClubData(districtId)

    val latestMonthClubRows = historicClubPerfGenerator.generateHistoricalClubData(
      districtId,
      progStartYearMonth
    )

    SuccessfulDistrictImportResult(districtId, clubCount, latestMonthClubRows)
  }

  def main(args: Array[String]): Unit = {

    try {
      applyRecentMonthsThresholdOverride(args)
    } catch {
      case e: IllegalArgumentException =>
        logger.error(e.getMessage)
        System.exit(1)
    }

    val districtFilter =
      try {
        parseDistrictsArg(args)
      } catch {
        case e: IllegalArgumentException =>
          logger.error(e.getMessage)
          System.exit(1)
          None // unreachable but needed for type inference
      }

    val dbConfig = DatabaseConfig.forConfig[JdbcProfile]("slick.dbs.tm_data")
    val dbRunner = new ConfigDBRunner(dbConfig)
    try {
      runMainImport(dbRunner, districtFilter)

    } catch {
      case e: Exception =>
        logger.error("Exception during main import", e)
        e.printStackTrace()
        System.exit(1)

    } finally dbConfig.db.close()

  }

  private def applyRecentMonthsThresholdOverride(args: Array[String]): Unit = {
    val overrideValue = resolveRecentMonthsThreshold(args)
    overrideValue.foreach { months =>
      logger.info(s"Overriding recent months threshold via configuration: $months month(s)")
      TMDataConstants.setRecentMonthsThreshold(months)
    }

    logger.info(
      s"Using recent months threshold ${TMDataConstants.getRecentMonthsThreshold} month(s). " +
        s"Override with $RecentMonthsFlag <value>, $RecentMonthsFlag=value, or env $RecentMonthsEnvVar."
    )
  }

  private def resolveRecentMonthsThreshold(args: Array[String]): Option[Int] = {
    val fromArgs = extractRecentMonthsArg(args.toList)
    val fromEnv  = sys.env.get(RecentMonthsEnvVar)

    fromArgs
      .map(value => parseRecentMonthsValue(value, s"command line flag $RecentMonthsFlag"))
      .orElse(fromEnv.map(value => parseRecentMonthsValue(value, s"environment variable $RecentMonthsEnvVar")))
  }

  private def parseRecentMonthsValue(value: String, source: String): Int = {
    val trimmed = value.trim
    trimmed.toIntOption match {
      case Some(months) if months >= 0 => months
      case _ =>
        throw new IllegalArgumentException(
          s"$source must be a non-negative integer, got '$value'"
        )
    }
  }

  private def extractRecentMonthsArg(args: List[String]): Option[String] = {
    @tailrec
    def loop(remaining: List[String]): Option[String] = {
      remaining match {
        case Nil => None
        case head :: tail if head.startsWith(RecentMonthsFlagWithValue) =>
          val value = head.substring(RecentMonthsFlagWithValue.length)
          if (value.isEmpty) {
            throw new IllegalArgumentException(s"$RecentMonthsFlag must supply a value")
          }
          Some(value)
        case flag :: value :: tail if flag == RecentMonthsFlag =>
          Some(value)
        case flag :: Nil if flag == RecentMonthsFlag =>
          throw new IllegalArgumentException(s"$RecentMonthsFlag requires a value")
        case _ :: tail =>
          loop(tail)
      }
    }

    loop(args)
  }

  private def parseDistrictsArg(args: Array[String]): Option[Set[String]] = {
    extractDistrictsArg(args.toList).map { value =>
      val districts = value.split(',').map(_.trim).filter(_.nonEmpty).toSet
      if (districts.isEmpty) {
        throw new IllegalArgumentException(s"$DistrictsFlag must contain at least one district ID")
      }
      logger.info(s"Filtering to districts: ${districts.mkString(", ")}")
      districts
    }
  }

  private def extractDistrictsArg(args: List[String]): Option[String] = {
    @tailrec
    def loop(remaining: List[String]): Option[String] = {
      remaining match {
        case Nil => None
        case head :: tail if head.startsWith(DistrictsFlagWithValue) =>
          val value = head.substring(DistrictsFlagWithValue.length)
          if (value.isEmpty) {
            throw new IllegalArgumentException(s"$DistrictsFlag must supply a value")
          }
          Some(value)
        case flag :: value :: tail if flag == DistrictsFlag =>
          Some(value)
        case flag :: Nil if flag == DistrictsFlag =>
          throw new IllegalArgumentException(s"$DistrictsFlag requires a value")
        case _ :: tail =>
          loop(tail)
      }
    }

    loop(args)
  }

  private def subtractMonths(yearMonth: YearMonth, monthsToSubtract: Int): YearMonth = {
    if (monthsToSubtract == 0) return yearMonth

    val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)
    val currentIndex     = allProgramMonths.indexOf(yearMonth.month)

    if (currentIndex == -1) {
      throw new IllegalArgumentException(s"Invalid month: ${yearMonth.month}")
    }

    var year       = yearMonth.year
    var monthIndex = currentIndex

    for (_ <- 1 to monthsToSubtract) {
      monthIndex -= 1
      if (monthIndex < 0) {
        monthIndex = allProgramMonths.length - 1
        year -= 1
      }
    }

    YearMonth(year, allProgramMonths(monthIndex))
  }

  def runMainImport(dbRunner: DBRunner, districtFilter: Option[Set[String]] = None): Unit = {

    val startYear = TMDataConstants.START_YEAR

    val clubInfoTable                      = new TMDataClubInfoTable(dbRunner)
    val clubInfoHistoricalTable            = new TMDataClubInfoHistoricalTable(dbRunner)
    val clubInfoWithHistory                = new ClubInfoWithHistory(clubInfoTable, clubInfoHistoricalTable, dbRunner)
    val clubInfoGenerator                  = new ClubInfoGenerator(clubInfoWithHistory)
    val districtSummaryHistoricalTable     = new TMDataDistrictSummaryHistoricalTable(dbRunner)
    val districtRegionTable                = new TMDataDistrictRegionTable(dbRunner)
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
    logger.info("Ensuring club info historical table exists")
    clubInfoHistoricalTable.createIfNotExists()
    logger.info("Ensuring historic club performance table exists")
    historicClubPerfTable.createIfNotExists()
    logger.info("Ensuring district summary historical table exists")
    districtSummaryHistoricalTable.createIfNotExists()
    logger.info("Ensuring district region table exists")
    districtRegionTable.createIfNotExists()

    logger.info("Generating historical overview data")

    districtSummaryHistoricalGenerator.generateHistoricalOverviewData(startYear)

    logger.info("Updating district region table from historical data")
    val updatedRegionCount = districtRegionTable.updateFromHistorical(districtSummaryHistoricalTable)
    logger.info(s"Updated $updatedRegionCount district region records")

    logger.info("Fetching district Ids")
    val allDistrictIds = {
      val allIds = districtSummaryHistoricalTable.allDistrictIds().sorted
      districtFilter match {
        case Some(filter) =>
          val filtered = allIds.filter(filter.contains)
          if (filtered.isEmpty) {
            logger.warn(s"No districts found matching filter: ${filter.mkString(", ")}")
          }
          filtered
        case None =>
          allIds
      }
    }

    logger.info("Found " + allDistrictIds.size + " districts")

    logger.info("Fetching latest month end dates by district")
    val latestMonthEndDatesByDistrict = historicClubPerfTable.latestDistrictMonthDates()

    logger.info("Found " + latestMonthEndDatesByDistrict.size + " districts")

    val districtCount = allDistrictIds.size

    logger.info(s"Running per district ($districtCount) imports sequentially")

    val results = allDistrictIds.sorted.zipWithIndex.map { case (districtId, index) =>
      val latest = latestMonthEndDatesByDistrict.get(districtId)
      val first  = districtSummaryHistoricalTable.firstOverviewDate(districtId)

      val progStartYearMonth = (first, latest) match {
        case (None, None) =>
          throw new IllegalStateException("No start date found for district " + districtId)
        case (Some(first), None) =>
          first
        case (_, Some(latest)) =>
          // When we have existing data, go back by (recentMonthsThreshold - 1) months
          // to ensure we reprocess all recent months
          val recentMonthsThreshold = TMDataConstants.getRecentMonthsThreshold
          val monthsToGoBack        = math.max(0, recentMonthsThreshold - 1)
          val adjusted              = subtractMonths(latest, monthsToGoBack)
          logger.debug(
            s"District $districtId: adjusting start from ${latest.toYYYYMM} back $monthsToGoBack months to ${adjusted.toYYYYMM} (recent months threshold: $recentMonthsThreshold)"
          )
          adjusted
      }

      try {
        val curCompleted    = index + 1
        val districtStartMs = System.currentTimeMillis()
        logger.info(s"District $districtId ($curCompleted/$districtCount): import starting")
        areaDownloader.generateHistoricalAreaData(districtId, startYear)
        divisionDownloader.generateHistoricalDivData(districtId, startYear)
        val res = generateDistrictData(
          clubInfoGenerator,
          historicClubPerfGenerator,
          districtId,
          progStartYearMonth
        )
        logger.info(
          s"District $districtId ($curCompleted/$districtCount): import complete in ${System.currentTimeMillis() - districtStartMs}ms"
        )

        // Only sleep if not the last district
        if (index < districtCount - 1) {
          Thread.sleep(10000) // sleep for 10 seconds between district imports to avoid rate limiting
        }

        res
      } catch {
        case e: Exception =>
          logger.error(s"Failed to load district data $districtId", e)
          FailedDistrictImportResult(districtId, e.toString)
      }
    }

    // separate results into successful and failed
    val successful = results.collect { case s: SuccessfulDistrictImportResult => s }
    val failed     = results.collect { case f: FailedDistrictImportResult => f }

    logger.info(s"Import complete: ${successful.size} successful, ${failed.size} failed")
    successful.foreach { s =>
      logger.debug(s.toString)
    }
    if (failed.nonEmpty) {
      logger.error(s"Failed imports (${failed.size}):")
      failed.foreach { f =>
        logger.error(f.toString)
      }
    }

    // Log summary of all date corrections made during import
    ToastmastersDateUtils.logCorrectionSummary()

  }
}
