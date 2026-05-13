package com.github.rorygraves.tm_data.data.club.perf

import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.data.district.historical.TMDataDistrictSummaryHistoricalTable
import com.github.rorygraves.tm_data.util.{DistrictUtil, TMUtil}
import com.github.rorygraves.tm_data.{DocumentType, TMDataConstants, TMDocumentDownloader, YearMonth}
import org.slf4j.LoggerFactory
import toasthost.utils.ToastmastersDateUtils

import java.time.LocalDate

/** Class to generate club data from the TI club reports */
class HistoricClubPerfGenerator(
    districtSummaryHistoricalTableDef: TMDataDistrictSummaryHistoricalTable,
    historicClubPerfTableDef: HistoricClubPerfTable
) {

  private val logger = LoggerFactory.getLogger("HistoricClubPerfGenerator")
  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      region: String,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint]
  ): TMClubDataPoint = {

    try {
      val clubNumber = data
        .getOrElse(
          "Club Number",
          data.getOrElse(
            "Club",
            throw new IllegalStateException(s"Field not found 'ClubNumber' in ${data.keys.mkString(",")}")
          )
        )
        .toInt
      val district = DistrictUtil.cleanDistrict(data("District"))

      def findPrev(programYear: Int, month: Int): Option[TMClubDataPoint] = {
        historicClubPerfTableDef.findByClubYearMonth(clubNumber, programYear, month)
      }

      val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)

      val dataKey = ClubMatchKey(programYear, month, clubNumber)

      def parseInt(s: String): Int = {
        if (s.isEmpty) 0 else s.toInt
      }

      val memBase            = parseInt(data("Mem. Base"))
      val activeMembers: Int = data("Active Members").toInt
      val membershipGrowth: Int = data.get("net_growth") match {
        case Some(value) => parseInt(value)
        case None        => activeMembers - memBase
      }

      val dcpData = ClubDCPData.fromDistrictClubReportCSV(programYear, data)

      def computeMonthlyGrowth(): Int = {
        if (month == 7) {
          dcpData.newMembers + dcpData.addNewMembers
        } else {
          // Expected for newly chartered clubs and the first month after import begins;
          // not actionable on a per-club basis, so keep it at DEBUG.
          val prevCount = findPrev(programYear, if (month == 1) 12 else month - 1) match {
            case Some(prev) =>
              prev.dcpData.newMembers + prev.dcpData.addNewMembers
            case None =>
              logger.debug(
                s"No previous month found for growth: club=$clubNumber year=$programYear month=$month"
              )
              0
          }
          dcpData.newMembers + dcpData.addNewMembers - prevCount
        }
      }

      val members30Sept =
        if (month == 7 || month == 8 || month == 9)
          activeMembers
        else
          findPrev(programYear, 9) match {
            case Some(prev) => prev.activeMembers
            case None =>
              logger.warn(
                s"No September row found for members_30_sept: club=$clubNumber year=$programYear month=$month - defaulting to 0; backfill required once Sep is imported"
              )
              0
          }

      val members31Mar =
        if (month == 7 || month == 8 || month == 9)
          0
        else if (month == 10 || month == 11 || month == 12 || month == 1 || month == 2 || month == 3)
          activeMembers
        else
          findPrev(programYear, 3) match {
            case Some(prev) => prev.activeMembers
            case None =>
              logger.warn(
                s"No March row found for members_31_mar: club=$clubNumber year=$programYear month=$month - defaulting to 0; backfill required once Mar is imported"
              )
              0
          }

      val monthlyGrowth = computeMonthlyGrowth()

      val awardsPerMember: Double =
        if (activeMembers > 0 && dcpData.totalAwards > 0) dcpData.totalAwards.toDouble / activeMembers else 0.0

      val csp             = data.getOrElse("CSP", "N")
      val baseEligibility = activeMembers > 19 || membershipGrowth > 2
      // From program year 2025 onwards TI also requires Club Success Plan submission (issue #7).
      val dcpEligibility: Boolean =
        if (programYear >= 2025) csp == "Y" && baseEligibility
        else baseEligibility

      TMClubDataPoint(
        programYear,
        month,
        monthEndDate,
        asOfDate,
        district,
        region,
        DistrictUtil.cleanDistrict(data("Division")),
        DistrictUtil.cleanDistrict(data("Area")),
        clubNumber,
        data("Club Name"),
        data("Club Status"),
        memBase,
        activeMembers,
        membershipGrowth,
        awardsPerMember,
        dcpEligibility,
        data("Goals Met").toInt,
        dcpData,
        data.get("Club Distinguished Status").flatMap(s => if (s.isEmpty) None else Some(s)),
        monthlyGrowth,
        members30Sept,
        members31Mar,
        clubDivDataPoints.get(dataKey).map(_.toClubData).getOrElse(TMDivClubDataPoint.empty),
        clubDistDataPoints(dataKey).toClubData,
        csp
      )
    } catch {
      case e: Exception => {
        logger.error(s"Error processing club data: ${e.getMessage}")
        logger.error(s"Data: ${data.mkString(" ")}")
        throw e
      }
    }
  }
  def generateHistoricalDivData(
      progYear: Int,
      month: Int,
      districtId: String
  ): List[TMDivClubDataPoint] = {
    logger.debug(s"generateHistoricalDivData($progYear, $month, $districtId)")
    TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.Division,
      Some(districtId),
      { case (year, month, asOfDate, data) =>
        // Convert calendar month to program month for correction utility
        val programMonth = ToastmastersDateUtils.calendarMonthToTMMonth(month)

        // Use the utility to correct program year if needed
        val correctedProgramYear = ToastmastersDateUtils.correctProgramYear(
          year,
          programMonth,
          asOfDate,
          Some("Division perf data")
        )

        TMDivClubDataPoint.fromDivisionReportCSV(correctedProgramYear, month, asOfDate, data)
      }
    )
  }

  def generateHistoricalClubDistData(
      progYear: Int,
      month: Int,
      districtId: String
  ): List[TMDistClubDataPoint] = {
    logger.debug(s"generateHistoricalClubDistData($progYear, $month, $districtId)")
    TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.DistrictPerformance,
      Some(districtId),
      { case (year, month, asOfDate, data) =>
        // Convert calendar month to program month for correction utility
        val programMonth = ToastmastersDateUtils.calendarMonthToTMMonth(month)

        // Use the utility to correct program year if needed
        val correctedProgramYear = ToastmastersDateUtils.correctProgramYear(
          year,
          programMonth,
          asOfDate,
          Some("District perf data")
        )

        TMDistClubDataPoint.fromDistrictReportCSV(correctedProgramYear, month, asOfDate, data)
      }
    )
  }

  /** Import historic month end data from TI.  Returns the number of rows in the last month of data processed */
  def generateHistoricalClubData(
      districtId: String,
      progStartYearMonth: YearMonth
  ): Int = {

    logger.debug(s"generateHistoricalClubData($districtId)")
    val startYear = progStartYearMonth.year
    val endYear   = TMUtil.currentProgramYear

    var lastMonthCount = 0
    (startYear to endYear).foreach { progYear =>
      logger.debug(f"Running historical club data import for year District $districtId-$progYear")
      val startMonthOpt = if (progYear == progStartYearMonth.year) Some(progStartYearMonth.month) else None
      lastMonthCount = downloadHistoricalClubData(progYear, districtId, startMonthOpt)
    }

//    outputClubData(districtId)

    lastMonthCount
  }

  def monthExists(progYear: Int, month: Int, districtId: String): Boolean = {
    historicClubPerfTableDef.existsByYearMonthDistrict(progYear, month, districtId)
  }

  val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

  def downloadHistoricalClubData(
      progYear: Int,
      districtId: String,
      startMonthOpt: Option[Int],
      single: Boolean = false
  ): Int = {

    var lastMonthCount = 0

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)

    val today = LocalDate.now()
    val monthsBase = (startMonthOpt match {
      case Some(startMonth) =>
        allProgramMonths.dropWhile(_ != startMonth)
      case None =>
        allProgramMonths
    }).filterNot(TMUtil.programMonthToSOMDate(progYear, _).isAfter(today))

    val months = if (single) monthsBase.take(1) else monthsBase

    var skippedExistingCount = 0
    var skippedFutureCount   = 0
    var processedCount       = 0

    months.foreach { month =>
      val targetMonth        = TMUtil.programMonthToSOMDate(progYear, month)
      val isRecent           = TMDataConstants.isRecentMonth(targetMonth)
      val monthAlreadyExists = monthExists(progYear, month, districtId)
      if (monthAlreadyExists && !isRecent) {
        skippedExistingCount += 1
      } else if (targetMonth.isAfter(LocalDate.now())) {
        skippedFutureCount += 1
      } else {
        processedCount += 1
        logger.debug(s"Processing District $districtId $progYear-$month ($targetMonth)")

        val divData  = generateHistoricalDivData(progYear, month, districtId)
        val distData = generateHistoricalClubDistData(progYear, month, districtId)

        val clubDivDataPoints  = divData.map(r => r.matchKey -> r).toMap
        val clubDistDataPoints = distData.map(r => r.matchKey -> r).toMap

        val regionOpt: Option[String] = {

          def findRegionByProgYearMonth(progYear: Int, month: Int): Option[String] = {
            districtSummaryHistoricalTableDef
              .searchBy(Some(districtId), Some(progYear), Some(month), limit = Some(1))
              .headOption
              .map(_.region)
          }

          findRegionByProgYearMonth(progYear, month) match {
            case Some(region) => Some(region)
            case None =>
              val (y2, m2) =
                if (month == 7) (progYear - 1, 6) else if (month == 1) (progYear, 12) else (progYear, month - 1)
              findRegionByProgYearMonth(y2, m2)
          }
        }

        lazy val region = regionOpt.getOrElse(
          throw new IllegalStateException(s"Unable to infer region for district $districtId $progYear $month")
        )

        logger.debug(s"  Downloading club data for D$districtId $progYear-$month")
        val monthData = reportDownloader(
          progYear,
          month,
          DocumentType.Club,
          Some(districtId),
          (year, month, asOfDate, rawData) => {
            // Convert calendar month to program month for correction utility
            val programMonth = ToastmastersDateUtils.calendarMonthToTMMonth(month)

            // Use the utility to correct program year if needed
            val correctedProgramYear = ToastmastersDateUtils.correctProgramYear(
              year,
              programMonth,
              asOfDate,
              Some("Club data")
            )

            fromDistrictClubReportCSV(
              correctedProgramYear,
              month,
              region,
              asOfDate,
              rawData,
              clubDivDataPoints,
              clubDistDataPoints
            )
          }
        )

        if (monthData.nonEmpty) {
          val monthDataCleaned = cleanMonthData(monthData)
          logger.debug(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")
          if (monthDataCleaned.length != monthData.length) {
            logger.warn(
              s"D$districtId $progYear-$month: ${monthData.length - monthDataCleaned.length} duplicate club rows removed (cleaned: ${monthDataCleaned.length})"
            )
          }

          if (monthDataCleaned.nonEmpty) {
            val count = historicClubPerfTableDef.insertOrUpdate(monthDataCleaned)
            logger.debug(s"  result $count - rows inserted/updated")
            lastMonthCount = count
          }
        }
      }
    }

    // Log summary
    if (skippedExistingCount > 0 || skippedFutureCount > 0) {
      logger.info(
        s"D$districtId club data $progYear: processed $processedCount months, skipped $skippedExistingCount existing, skipped $skippedFutureCount future"
      )
    } else if (processedCount > 0) {
      logger.info(s"D$districtId club data $progYear: processed $processedCount months")
    }

    lastMonthCount
  }

  private def cleanMonthData(monthData: List[TMClubDataPoint]): List[TMClubDataPoint] = {
    monthData.groupBy(_.clubNumber).values.map(_.last).toList
  }

  def outputClubData(districtId: String): Unit = {
    logger.info(s"Club performance data processing completed for district $districtId - data stored in database")
  }
}
