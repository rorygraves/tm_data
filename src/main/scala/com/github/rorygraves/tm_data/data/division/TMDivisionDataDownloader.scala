package com.github.rorygraves.tm_data.data.division

import com.github.rorygraves.tm_data.{DocumentType, TMDataConstants}
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.{DistrictUtil, TMUtil}
import org.slf4j.LoggerFactory
import toasthost.utils.ToastmastersDateUtils

import java.time.LocalDate

/** Class to generate club data from the TI club reports */
class TMDivisionDataDownloader(historicDivisionPerfTable: HistoricDivisionPerfTable) {

  private val logger = LoggerFactory.getLogger("TMDivisionDataDownloader")

  def divisionPointFromDivisionReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String]
  ): TMDivisionDataPoint = {
    TMDivisionDataPoint(
      programYear,
      month,
      TMUtil.computeMonthEndDate(programYear, month),
      asOfDate,
      DistrictUtil.cleanDistrict(data("District")),
      DistrictUtil.cleanDistrict(data("Division")),
      data("Division Club Base").toInt,
      data("Division Paid Club Goal for Dist.").toInt,
      data("Division Paid Club Goal for Select Dist.").toInt,
      data("Division Paid Club Goal for Pres. Dist.").toInt,
      data("Total Paid Division Clubs").toInt,
      data("Division Dist. Club Goal for Dist.").toInt,
      data("Division Dist. Club Goal for Select Dist.").toInt,
      data("Division Dist. Club Goal for Pres. Dist.").toInt,
      data("Total Dist. Division Clubs").toInt,
      data("Distinguished Division")
    )
  }

  /** Import historic month end data from TI.  Returns the number of rows in the last month of data processed */
  def generateHistoricalDivData(
      districtId: String,
      progStartYear: Int
  ): Unit = {

    logger.debug(s"generateHistoricalDivData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    val existingYearMonths = historicDivisionPerfTable.getYearMonthsForDistrict(districtId).toSet

    val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

    var skippedCount    = 0
    var downloadedCount = 0
    val skippedYears    = scala.collection.mutable.Set[Int]()

    (startYear to endYear).foreach { progYear =>
      allProgramMonths.foreach { progMonth =>
        val targetMonth        = TMUtil.programMonthToSOMDate(progYear, progMonth)
        val isRecent           = TMDataConstants.isRecentMonth(targetMonth)
        val monthAlreadyExists = existingYearMonths.contains((progYear, progMonth))

        // Skip download if: future month, OR (historical data exists and not recent)
        if (targetMonth.isAfter(LocalDate.now()) || (!isRecent && monthAlreadyExists)) {
          if (monthAlreadyExists) {
            skippedCount += 1
            skippedYears += progYear
          }
        } else {
          logger.debug(f"Downloading division data for D$districtId $progYear-$progMonth")
          downloadHistoricalDivData(districtId, progYear, progMonth)
          downloadedCount += 1
        }
      }
    }

    if (skippedCount > 0) {
      logger.info(
        s"D$districtId division data: skipped $skippedCount months (years: ${skippedYears.toSeq.sorted.mkString(", ")}), downloaded $downloadedCount months"
      )
    } else {
      logger.info(s"D$districtId division data: downloaded $downloadedCount months")
    }

    def downloadHistoricalDivData(
        districtId: String,
        progYear: Int,
        month: Int
    ): Unit = {

      // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)

      val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
      logger.debug(s"  Downloading div data for D$districtId $progYear-$month ($targetMonth)")
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
            Some("Division data")
          )

          divisionPointFromDivisionReportCSV(correctedProgramYear, month, asOfDate, rawData)
        }
      )

      val monthData = rawMonthData.distinct

      if (monthData.nonEmpty) {
        logger.debug(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")

        if (monthData.nonEmpty) {
          val count = historicDivisionPerfTable.insertOrUpdate(monthData)
          logger.debug(s"  result $count - rows inserted/updated")
        }
      }
    }
  }
}
