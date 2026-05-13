package com.github.rorygraves.tm_data.data.area.perf

import com.github.rorygraves.tm_data.{DocumentType, TMDataConstants}
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.{DistrictUtil, TMUtil}
import org.slf4j.LoggerFactory
import toasthost.utils.ToastmastersDateUtils

import java.time.LocalDate

/** Class to generate club data from the TI club reports */
class TMAreaDataDownloader(historicAreaPerfTableDef: HistoricAreaPerfTable) {

  private val logger = LoggerFactory.getLogger("TMAreaDataDownloader")

  def areaPointFromDivisionReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String]
  ): TMAreaDataPoint = {
    TMAreaDataPoint(
      programYear,
      month,
      TMUtil.computeMonthEndDate(programYear, month),
      asOfDate,
      DistrictUtil.cleanDistrict(data("District")),
      data("Division"),
      DistrictUtil.cleanDistrict(data("Area")),
      data("Area Club Base").toInt,
      data("Area Paid Club Goal for Dist.").toInt,
      data("Area Paid Club Goal for Select Dist.").toInt,
      data("Area Paid Club Goal for Pres. Dist.").toInt,
      data("Total Paid Area Clubs").toInt,
      data("Area Dist. Club Goal for Dist.").toInt,
      data("Area Dist. Club Goal for Select Dist.").toInt,
      data("Area Dist. Club Goal for Pres. Dist.").toInt,
      data("Total Dist. Area Clubs").toInt,
      data("Distinguished Area")
    )
  }

  /** Import historic month end data from TI.  Returns the number of rows in the last month of data processed */
  def generateHistoricalAreaData(
      districtId: String,
      progStartYear: Int
  ): Unit = {

    logger.debug(s"generateHistoricalAreaData($districtId, progStartYear=$progStartYear)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    val existingYearMonths = historicAreaPerfTableDef.getYearMonthsForDistrict(districtId).toSet

    var skippedCount    = 0
    var downloadedCount = 0
    val skippedYears    = scala.collection.mutable.Set[Int]()

    (startYear to endYear).foreach { progYear =>
      List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6).foreach { month =>
        val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
        val isRecent    = TMDataConstants.isRecentMonth(targetMonth)
        val exists      = existingYearMonths.contains((progYear, month))

        // Skip download if: future month, OR (historical data exists and not recent)
        if (targetMonth.isAfter(LocalDate.now()) || (!isRecent && exists)) {
          if (exists) {
            skippedCount += 1
            skippedYears += progYear
          }
        } else {
          logger.debug(f"Downloading area data for D$districtId $progYear-$month (isRecent=$isRecent, exists=$exists)")
          downloadHistoricalAreaDataForMonth(districtId, progYear, month)
          downloadedCount += 1
        }
      }
    }

    if (skippedCount > 0) {
      logger.info(
        s"D$districtId area data: skipped $skippedCount months (years: ${skippedYears.toSeq.sorted.mkString(", ")}), downloaded $downloadedCount months"
      )
    } else {
      logger.info(s"D$districtId area data: downloaded $downloadedCount months")
    }
  }

  def downloadHistoricalAreaDataForMonth(districtId: String, progYear: Int, month: Int): Unit = {

    val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
    logger.debug(s"  Processing area data for D$districtId $progYear-$month ($targetMonth)")

    val rawMonthData = reportDownloader(
      progYear,
      month,
      DocumentType.Division,
      Some(districtId),
      (year, month, asOfDate, rawData) => {
        // Convert calendar month to program month for correction utility
        // Calendar: Jan=1...Dec=12 -> Program: July=1...June=12
        val programMonth = ToastmastersDateUtils.calendarMonthToTMMonth(month)

        // Use the utility to correct program year if needed
        val correctedProgramYear = ToastmastersDateUtils.correctProgramYear(
          year,
          programMonth,
          asOfDate,
          Some("Area data")
        )

        areaPointFromDivisionReportCSV(
          correctedProgramYear,
          month,
          asOfDate,
          rawData
        )
      }
    )

    val monthData = rawMonthData.distinct

    if (monthData.nonEmpty) {
      logger.debug(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")
      val count = historicAreaPerfTableDef.insertOrUpdate(monthData)
      logger.debug(s"  result $count - rows inserted/updated")
    } else {
      logger.debug(s"  No data for D$districtId $progYear-$month")
    }
  }
}
