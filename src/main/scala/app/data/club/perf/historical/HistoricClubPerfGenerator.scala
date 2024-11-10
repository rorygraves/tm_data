package app.data.club.perf.historical

import app.TMDocumentDownloader.reportDownloader
import app.data.club.perf.historical.data.{
  HistoricClubPerfTableDef,
  TMClubDataPoint,
  TMDistClubDataPoint,
  TMDivClubDataPoint
}
import app.db.DataSource
import app.util.TMUtil
import app.{DocumentType, TMDocumentDownloader}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import scala.jdk.CollectionConverters.IterableHasAsJava

/** Class to generate club data from the TI club reports */
object HistoricClubPerfGenerator {

  def generateHistoricalDivData(
      progYear: Int,
      month: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDivClubDataPoint] = {
    TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.Division,
      Some(districtId),
      cacheFolder,
      { case (year, month, asOfDate, data) =>
        TMDivClubDataPoint.fromDivisionReportCSV(year, month, asOfDate, data)
      }
    )
  }

  def generateHistoricalClubDistData(
      progYear: Int,
      month: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDistClubDataPoint] = {
    TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.DistrictPerformance,
      Some(districtId),
      cacheFolder,
      { case (year, month, asOfDate, data) =>
        TMDistClubDataPoint.fromDistrictReportCSV(year, month, asOfDate, data)
      }
    )
  }

  def generateHistoricalClubData(cacheFolder: String, districtId: Int, dataSource: DataSource): Unit = {

    val startYear = 2012
    val endYear   = 2024

    (startYear to endYear).foreach { progYear =>
      println(f"Running historical club data import for year $progYear")
      downloadHistoricalClubData(progYear, districtId, cacheFolder, dataSource)
    }

    outputClubData(dataSource, districtId)
  }

  def monthExists(progYear: Int, month: Int, districtId: Int, dataSource: DataSource): Boolean = {
    HistoricClubPerfTableDef.existsByYearMonthDistrict(dataSource, progYear, month, districtId)
  }

  def downloadHistoricalClubData(
      progYear: Int,
      districtId: Int,
      cacheFolder: String,
      dataSource: DataSource
  ): Unit = {

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)
    val months = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

    months.foreach { month =>
      val targetMonth = TMUtil.programMonthToSOMDate(progYear, month)
      println(s"Processing $districtId $progYear-$month ($targetMonth)")
      val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(2))
      val monthAlreadyExists = monthExists(progYear, month, districtId, dataSource)
      if (monthAlreadyExists && !isRecent) {
        println(f"  Skipping month $month for year $progYear - already exists")
      } else if (targetMonth.isAfter(LocalDate.now())) {
        println(f"  Skipping month $month for year $progYear - it is in the future")
      } else {
        println(f"  Querying TI month $month for year $progYear")

        val divData  = generateHistoricalDivData(progYear, month, districtId, cacheFolder)
        val distData = generateHistoricalClubDistData(progYear, month, districtId, cacheFolder)

        val clubDivDataPoints  = divData.map(r => r.matchKey -> r).toMap
        val clubDistDataPoints = distData.map(r => r.matchKey -> r).toMap

        val monthData = reportDownloader(
          progYear,
          month,
          DocumentType.Club,
          Some(districtId),
          cacheFolder,
          (year, month, asOfDate, rawData) => {
            TMClubDataPoint.fromDistrictClubReportCSV(
              year,
              month,
              asOfDate,
              rawData,
              clubDivDataPoints,
              clubDistDataPoints,
              dataSource
            )
          }
        )
        monthData.foreach { row =>
          dataSource.run(implicit conn => {
            if (monthAlreadyExists) {
              println("Updating existing row")
              conn.update(row, HistoricClubPerfTableDef)
            } else
              conn.insert(row, HistoricClubPerfTableDef)
          })
        }
      }
    }
  }

  def outputClubData(dataSource: DataSource, districtId: Int): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      val data = HistoricClubPerfTableDef.searchByDistrict(dataSource, districtId.toString).sorted

      // output the headers
      printer.printRecord(HistoricClubPerfTableDef.columns.map(_.name).asJava)
      // output the rows
      data.foreach { tmClubPoint =>
        val rowValues = HistoricClubPerfTableDef.columns.map(_.csvExportFn(tmClubPoint))
        printer.printRecord(rowValues.asJava)
      }

      val writer = new PrintWriter(new File(s"data/club_data_$districtId.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()

  }
}
