package app.club.perf.historical

import app.TMDocumentDownloader.reportDownloader
import app.club.perf.historical.data.{
  HistoricClubPerfTableDef,
  TMClubDataPoint,
  TMDistClubDataPoint,
  TMDivClubDataPoint
}
import app.db.DataSource
import app.{DocumentType, TMDocumentDownloader}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import scala.collection.immutable
import scala.jdk.CollectionConverters.IterableHasAsJava

/** Class to generate club data from the TI club reports */
object HistoricClubPerfGenerator {

  def generateHistoricalDivData(
      progYear: Int,
      month: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDivClubDataPoint] = {
    // club div data
    val clubDivData = TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.Division,
      districtId,
      cacheFolder,
      { case (year, month, asOfDate, data) =>
        TMDivClubDataPoint.fromDivisionReportCSV(year, month, asOfDate, data)
      }
    )

    println("Division data: " + clubDivData.size)
    clubDivData

  }

  def generateHistoricalClubDistData(
      progYear: Int,
      month: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDistClubDataPoint] = {
    // club div data
    val clubDistData = TMDocumentDownloader.reportDownloader(
      progYear,
      month: Int,
      DocumentType.DistrictPerformance,
      districtId,
      cacheFolder,
      { case (year, month, asOfDate, data) =>
        try {
          TMDistClubDataPoint.fromDistrictReportCSV(year, month, asOfDate, data)
        } catch {
          case e: Exception =>
            println(s"Failed to parse district club data for $year $month $asOfDate")
            println(data)
            throw e
        }
      }
    )

    clubDistData

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

    var clubData: List[TMClubDataPoint] = List.empty

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)
    val months = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

    months.foreach { month =>
      val targetMonth = LocalDate.of(if (month < 7) progYear + 1 else progYear, month, 1)
      println(s"Processing $districtId $progYear-$month ($targetMonth)")
      val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(2))
      val monthAlreadyExists = monthExists(progYear, month, districtId, dataSource)
      if (monthAlreadyExists && !isRecent) {
        println(f"  Skipping month $month for year $progYear - already exists")
      } else if (targetMonth.isAfter(LocalDate.now())) {
        println(f"  Skipping month $month for year $progYear - it is in the future")
      } else {
        val divData  = generateHistoricalDivData(progYear, month, districtId, cacheFolder)
        val distData = generateHistoricalClubDistData(progYear, month, districtId, cacheFolder)

        val clubDivDataPoints  = divData.map(r => r.matchKey -> r).toMap
        val clubDistDataPoints = distData.map(r => r.matchKey -> r).toMap

        val monthData = reportDownloader(
          progYear,
          month,
          DocumentType.Club,
          districtId,
          cacheFolder,
          (year, month, asOfDate, rawData) => {

            val dp = TMClubDataPoint.fromDistrictClubReportCSV(
              year,
              month,
              asOfDate,
              rawData,
              clubDivDataPoints,
              clubDistDataPoints
            )

            dp
          }
        )
        val enhancedMonth = enhanceTMData(monthData, dataSource)
        enhancedMonth.foreach { row =>
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

  def enhanceTMData(
      data: List[TMClubDataPoint],
      dataSource: DataSource
  ): immutable.Iterable[TMClubDataPoint] = {

    data.flatMap { dp => enhanceClubData(dp.clubNumber, List(dp), dataSource) }
  }

  def enhanceClubData(
      clubNumber: String,
      clubRows: Seq[TMClubDataPoint],
      dataSource: DataSource
  ): Seq[TMClubDataPoint] = {

    def findPrev(programYear: Int, month: Int): Option[TMClubDataPoint] = {
      HistoricClubPerfTableDef.findByClubYearMonth(dataSource, clubNumber, programYear, month)
    }

    val updatedRows = clubRows.map { row =>
      if (row.month == 7) {
        row.monthlyGrowth = row.dcpData.newMembers + row.dcpData.addNewMembers
      } else {
//        val prevMonth = rowMap.get(
//          (row.programYear, (if (row.month == 1) 12 else row.month - 1))
//        )
        val prevMonth = findPrev(row.programYear, if (row.month == 1) 12 else row.month - 1)

        val prevCount = prevMonth match {
          case Some(prev) =>
            prev.dcpData.newMembers + prev.dcpData.addNewMembers
          case None =>
            println(
              s"Warning! No previous month found for growth for Year: ${row.programYear} Club: $clubNumber Month: ${row.month}"
            )
            0
        }
        row.monthlyGrowth = row.dcpData.newMembers + row.dcpData.addNewMembers - prevCount

      }
      // 30SeptMembers
      if (row.month == 7 || row.month == 8 || row.month == 9) {
        row.members30Sept = row.activeMembers
      } else {
//        val earlierVal =
//          rowMap.get((row.programYear, 9)).map(_.activeMembers).getOrElse(-1)
        val earlierVal =
          findPrev(row.programYear, 9).map(_.activeMembers).getOrElse(-1)
        row.members30Sept = earlierVal
      }

      // 31MarMembers
      if (row.month == 7 || row.month == 8 || row.month == 9) {
        row.members31Mar = 0
      } else if (
        row.month == 10 || row.month == 11 || row.month == 12 || row.month == 1 || row.month == 2 || row.month == 3
      ) {
        row.members31Mar = row.activeMembers
      } else {
//        val earlierVal =
//          rowMap.get((row.programYear, 3)).map(_.activeMembers).getOrElse(-1)
        val earlierVal =
          findPrev(row.programYear, 3).map(_.activeMembers).getOrElse(-1)
        row.members31Mar = earlierVal
      }

      row
    }
    updatedRows
  }

}
