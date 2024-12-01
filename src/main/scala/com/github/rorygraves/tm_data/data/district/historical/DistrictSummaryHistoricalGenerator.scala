package com.github.rorygraves.tm_data.data.district.historical

import app.db.{Connection, DataSource}
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.util.TMUtil
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import scala.jdk.CollectionConverters.IterableHasAsJava

object DistrictSummaryHistoricalGenerator {

  def generateHistoricalOverviewData(cacheFolder: String, dataSource: DataSource): Unit = {

    val startYear = 2012
    val endYear   = TMUtil.currentProgramYear

    (startYear to endYear).foreach { progYear =>
      println(f"Running historical overview data import for year $progYear")
      downloadHistoricalOverviewData(progYear, cacheFolder, dataSource)
    }

    outputOverviewData(dataSource)
  }

  def outputOverviewData(dataSource: DataSource): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      val data = dataSource.run(conn => DistrictSummaryHistoricalTableDef.searchBy(conn).sorted)

      // output the headers
      printer.printRecord(DistrictSummaryHistoricalTableDef.columns.map(_.name).asJava)
      // output the rows
      data.foreach { tmClubPoint =>
        val rowValues = DistrictSummaryHistoricalTableDef.columns.map(_.csvExportFn(tmClubPoint))
        printer.printRecord(rowValues.asJava)
      }

      val writer = new PrintWriter(new File(s"data/district_overview.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()

  }

  def downloadHistoricalOverviewData(
      progYear: Int,
      cacheFolder: String,
      dataSource: DataSource
  ): Unit = {

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)
    val months = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

    months.foreach { month =>
      val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
      println(s"Processing district summary $progYear-$month ($targetMonth)")
      val isRecent = targetMonth.isAfter(LocalDate.now().minusMonths(2))
      val monthAlreadyExists = dataSource.run(conn => {
        monthExists(progYear, month, conn)
      })
      if (monthAlreadyExists && !isRecent) {
        println(f"  Skipping month $month for year $progYear - already exists")
      } else if (targetMonth.isAfter(LocalDate.now())) {
        println(f"  Skipping month $month for year $progYear - it is in the future")
      } else {
        println(f"  Querying TI month $month for year $progYear")

        val monthData = reportDownloader(
          progYear,
          month,
          DocumentType.Overview,
          None,
          cacheFolder,
          (year, month, asOfDate, rawData) => {
            DistrictSummaryHistoricalDataPoint.fromOverviewReportCSV(
              year,
              month,
              asOfDate,
              rawData
            )
          }
        )

        println(s"Processing month data ${monthData.length}")
        dataSource.transaction(implicit conn => {
          monthData.foreach { row =>
            if (monthAlreadyExists) {
              conn.update(row, DistrictSummaryHistoricalTableDef)
            } else
              conn.insert(row, DistrictSummaryHistoricalTableDef)
          }
        })
        println(s"Processing complete")
      }
    }
  }

  def monthExists(progYear: Int, month: Int, conn: Connection): Boolean = {
    DistrictSummaryHistoricalTableDef.existsByYearMonth(conn, progYear, month)
  }
}
