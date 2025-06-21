package com.github.rorygraves.tm_data.data.area.perf

import com.github.rorygraves.tm_data.DocumentType
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.{DistrictUtil, TMUtil}
import org.slf4j.LoggerFactory

import java.time.LocalDate

/** Class to generate club data from the TI club reports */
class TMAreaDataDownloader(historicAreaPerfTableDef: HistoricAreaPerfTable) {

  private val logger = LoggerFactory.getLogger(getClass)

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
      cacheFolder: String,
      districtId: String,
      progStartYear: Int
  ): Unit = {

    logger.info(s"generateHistoricalAreaData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    val existingYearMonths = historicAreaPerfTableDef.getYearMonthsForDistrict(districtId).toSet

    (startYear to endYear).foreach { progYear =>
      List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6).foreach { month =>
        val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
        val isRecent    = targetMonth.isAfter(LocalDate.now().minusMonths(2))
        val exists      = existingYearMonths.contains((progYear, month))

        if (!targetMonth.isAfter(LocalDate.now()) || (!isRecent && exists)) {} else {
          logger.info(f"Running historical area data import for year District $districtId-$progYear")
          downloadHistoricalAreaDataForMonth(districtId, progYear, month, cacheFolder)
        }
      }
    }
  }

  def downloadHistoricalAreaDataForMonth(districtId: String, progYear: Int, month: Int, cacheFolder: String): Unit = {

    val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
    logger.info(s"  Processing area data for D$districtId $progYear-$month ($targetMonth)")
    val rawMonthData = reportDownloader(
      progYear,
      month,
      DocumentType.Division,
      Some(districtId),
      cacheFolder,
      (year, month, asOfDate, rawData) => {
        areaPointFromDivisionReportCSV(
          year,
          month,
          asOfDate,
          rawData
        )
      }
    )

    val monthData = rawMonthData.distinct

    if (monthData.nonEmpty) {
      logger.info(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")
      val count = historicAreaPerfTableDef.insertOrUpdate(monthData)
      logger.info(s"  result $count - rows inserted/updated")
    }
  }
}
