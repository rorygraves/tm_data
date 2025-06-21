package com.github.rorygraves.tm_data.data.division

import com.github.rorygraves.tm_data.DocumentType
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.{DistrictUtil, TMUtil}
import org.slf4j.LoggerFactory

import java.time.LocalDate

/** Class to generate club data from the TI club reports */
class TMDivisionDataDownloader(historicDivisionPerfTable: HistoricDivisionPerfTable) {

  private val logger = LoggerFactory.getLogger(getClass)

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
      cacheFolder: String,
      districtId: String,
      progStartYear: Int
  ): Unit = {

    logger.info(s"generateHistoricalAreaData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    val existingYearMonths = historicDivisionPerfTable.getYearMonthsForDistrict(districtId).toSet

    val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

    (startYear to endYear).foreach { progYear =>
      allProgramMonths.foreach { progMonth =>
        val targetMonth        = TMUtil.programMonthToSOMDate(progYear, progMonth)
        val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(2))
        val monthAlreadyExists = existingYearMonths.contains((progYear, progMonth))
        if (isRecent || !monthAlreadyExists) {
          downloadHistoricalDivData(districtId, progYear, progMonth, cacheFolder)
        }
      }
    }

    def downloadHistoricalDivData(
        districtId: String,
        progYear: Int,
        month: Int,
        cacheFolder: String
    ): Unit = {

      // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)

      val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
      logger.info(s"  Downloading div data for D$districtId $progYear-$month ($targetMonth)")
      val rawMonthData = reportDownloader(
        progYear,
        month,
        DocumentType.Division,
        Some(districtId),
        cacheFolder,
        (year, month, asOfDate, rawData) => {
          divisionPointFromDivisionReportCSV(year, month, asOfDate, rawData)
        }
      )

      val monthData = rawMonthData.distinct

      if (monthData.nonEmpty) {
        logger.info(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")

        if (monthData.nonEmpty) {
          val count = historicDivisionPerfTable.insertOrUpdate(monthData)
          logger.info(s"  result $count - rows inserted/updated")
        }
      }
    }
  }
}
