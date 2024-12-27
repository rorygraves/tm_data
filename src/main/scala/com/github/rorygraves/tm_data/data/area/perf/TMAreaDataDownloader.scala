package com.github.rorygraves.tm_data.data.area.perf

import com.github.rorygraves.tm_data.DocumentType
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.TMUtil
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
      data("District"),
      data("Division"),
      data("Area"),
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
      progStartYear: Int,
      progStartMonth: Int
  ): Int = {

    logger.info(s"generateHistoricalAreaData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    var lastMonthCount = 0
    (startYear to endYear).foreach { progYear =>
      println(f"Running historical area data import for year District $districtId-$progYear")
      val startMonthOpt = if (progYear == progStartYear) Some(progStartMonth) else None
      lastMonthCount = downloadHistoricalAreaData(progYear, districtId, startMonthOpt, cacheFolder)
    }

    lastMonthCount
  }

  def monthExists(progYear: Int, month: Int, districtId: String): Boolean = {
    historicAreaPerfTableDef.existsByYearMonthDistrict(progYear, month, districtId)
  }

  val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

  def downloadHistoricalAreaData(
      progYear: Int,
      districtId: String,
      startMonthOpt: Option[Int],
      cacheFolder: String
  ): Int = {

    var lastMonthCount = 0

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)

    val today = LocalDate.now()
    val months = (startMonthOpt match {
      case Some(startMonth) =>
        allProgramMonths.dropWhile(_ != startMonth)
      case None =>
        allProgramMonths
    }).filterNot(TMUtil.programMonthToSOMDate(progYear, _).isAfter(today))

    months.foreach { month =>
      val targetMonth        = TMUtil.programMonthToSOMDate(progYear, month)
      val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(2))
      val monthAlreadyExists = monthExists(progYear, month, districtId)
      if (monthAlreadyExists && !isRecent) {
        logger.info(f"Skipping District $districtId $progYear-$month - already exists")
      } else if (targetMonth.isAfter(LocalDate.now())) {
        logger.info(f"Skipping month District $districtId $progYear-$month  - it is in the future")
      } else {
        logger.info(s"Processing District $districtId $progYear-$month ($targetMonth)")

        logger.info(s"  Downloading club data for D$districtId $progYear-$month")
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

          if (monthData.nonEmpty) {
            val count = historicAreaPerfTableDef.insertOrUpdate(monthData)
            logger.info(s"  result $count - rows inserted/updated")
            lastMonthCount = count
          }
        }
      }
    }
    lastMonthCount
  }
}
