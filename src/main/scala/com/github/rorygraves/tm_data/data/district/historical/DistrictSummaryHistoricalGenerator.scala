package com.github.rorygraves.tm_data.data.district.historical

import com.github.rorygraves.tm_data.DocumentType
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.TMUtil
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter}
import java.time.LocalDate

class DistrictSummaryHistoricalGenerator(
    districtSummaryHistoricalTableDef: TMDataDistrictSummaryHistoricalTable
) {

  val logger = LoggerFactory.getLogger(getClass)

  def generateHistoricalOverviewData(cacheFolder: String, startYear: Int = 2012): Unit = {

    val endYear = TMUtil.currentProgramYear

    (startYear to endYear).foreach { progYear =>
      logger.info(f"Running historical overview data import for year $progYear")
      downloadHistoricalOverviewData(progYear, cacheFolder)
    }

    outputOverviewData()
  }

  def outputOverviewData(): Unit = {
    try {

      val data    = districtSummaryHistoricalTableDef.searchBy().sorted
      val content = districtSummaryHistoricalTableDef.rowsToCSV(data)
      val writer  = new PrintWriter(new File(s"data/district_overview.csv"))
      writer.write(content)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    }

  }

  def downloadHistoricalOverviewData(
      progYear: Int,
      cacheFolder: String
  ): Unit = {

    val today = LocalDate.now()
    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)
    val months =
      List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6).filterNot(TMUtil.programMonthToSOMDate(progYear, _).isAfter(today))

    var skippedExistingCount = 0
    var skippedFutureCount = 0
    var processedCount = 0

    months.foreach { month =>
      val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
      val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(1)) // Changed from 2 to 1 month
      val monthAlreadyExists = monthExists(progYear, month)
      if (monthAlreadyExists && !isRecent) {
        skippedExistingCount += 1
      } else if (targetMonth.isAfter(LocalDate.now())) {
        skippedFutureCount += 1
      } else {
        processedCount += 1
        logger.info(s"Processing district summary $progYear-$month ($targetMonth)")

        val monthData = reportDownloader(
          progYear,
          month,
          DocumentType.Overview,
          None,
          cacheFolder,
          (year, month, asOfDate, rawData) => {
            TMDistrictSummaryDataPoint.fromOverviewReportCSV(
              year,
              month,
              asOfDate,
              rawData
            )
          }
        )

        logger.info(s"Processing month data ${monthData.length}")
        val res = districtSummaryHistoricalTableDef.insertOrUpdate(monthData)
        logger.info(s"  $res inserted or updated")
        logger.info(s"Processing complete")
      }
    }
    
    // Log summary
    if (skippedExistingCount > 0 || skippedFutureCount > 0) {
      logger.info(s"District summary $progYear: processed $processedCount months, skipped $skippedExistingCount existing, skipped $skippedFutureCount future")
    } else if (processedCount > 0) {
      logger.info(s"District summary $progYear: processed $processedCount months")
    }
  }

  def monthExists(progYear: Int, month: Int): Boolean = {
    districtSummaryHistoricalTableDef.existsByYearMonth(progYear, month)
  }
}
