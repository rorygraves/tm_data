package app.club.perf.historical

import app.TMDocumentDownloader.reportDownloader
import app.club.perf.historical.HistoricClubPerfOutputFormat.clubColumnGenerator
import app.club.perf.historical.data.{ClubMatchKey, TMClubDataPoint, TMDistClubDataPoint, TMDivClubDataPoint}
import app.{DocumentType, TMDocumentDownloader}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.jdk.CollectionConverters.IterableHasAsJava
import java.io.{File, PrintWriter, StringWriter}
import scala.collection.immutable

/** Class to generate club data from the TI club reports */
object HistoricClubPerfGenerator {

  def generateHistoricalDivData(
      startYear: Int,
      endYear: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDivClubDataPoint] = {
    // club div data
    val clubDivData = TMDocumentDownloader.reportDownloader(
      startYear,
      endYear,
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
      startYear: Int,
      endYear: Int,
      districtId: Int,
      cacheFolder: String
  ): List[TMDistClubDataPoint] = {
    // club div data
    val clubDistData = TMDocumentDownloader.reportDownloader(
      startYear,
      endYear,
      DocumentType.DistrictPerformance,
      districtId,
      cacheFolder,
      { case (year, month, asOfDate, data) =>
        try {
          TMDistClubDataPoint.fromDistrictReportCSV(year, month, asOfDate, data)
        } catch {
          case e: Exception =>
            println(s"Failed to parse district club data for $year $month $asOfDate")
            println("Keys = " + data.keys.mkString(","))
            println(data)
            throw e
        }
      }
    )

    clubDistData

  }

  def generateHistoricalClubData(cacheFolder: String, districtId: Int): Unit = {

    val startYear = 2012
    val endYear   = 2024

//    val startYear = 2019
//    val endYear   = 2019

    val divData  = generateHistoricalDivData(startYear, endYear, districtId, cacheFolder)
    val distData = generateHistoricalClubDistData(startYear, endYear, districtId, cacheFolder)

    val clubDivMap  = divData.map(r => r.matchKey -> r).toMap
    val clubDistMap = distData.map(r => r.matchKey -> r).toMap

    val clubData = generateHistoricalClubData(startYear, endYear, districtId, clubDivMap, clubDistMap, cacheFolder)
    println(f"Generated Club data: ${clubData.size}%,d row")
    outputClubData(clubData.sorted, districtId)
  }

  def generateHistoricalClubData(
      startYear: Int,
      endYear: Int,
      districtId: Int,
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint],
      cacheFolder: String
  ): List[TMClubDataPoint] = {
    // club data
    val clubData = reportDownloader(
      startYear,
      endYear,
      DocumentType.Club,
      districtId,
      cacheFolder,
      (year, month, asOfDate, rawData) => {
        TMClubDataPoint.fromDistrictClubReportCSV(
          year,
          month,
          asOfDate,
          rawData,
          clubDivDataPoints,
          clubDistDataPoints
        )
      }
    )
    val enhancedData = enhanceTMData(clubData).toList

    enhancedData
  }

  def outputClubData(data: List[TMClubDataPoint], districtId: Int): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      // output the headers
      printer.printRecord(clubColumnGenerator.map(_.name).asJava)
      // output the rows
      data.foreach { tmclubpoint =>
        val rowValues = clubColumnGenerator.map(_.calculation(tmclubpoint))
        printer.printRecord(rowValues.asJava)
      }

      // log the output for debug
      println(out.toString.take(1500))

      val writer = new PrintWriter(new File(s"data/club_data_$districtId.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()

  }

  def enhanceTMData(
      data: List[TMClubDataPoint]
  ): immutable.Iterable[TMClubDataPoint] = {

    data.groupBy(_.clubNumber).flatMap { case (clubNumber, clubRows) =>
      enhanceClubData(clubNumber, clubRows)
    }
  }

  def enhanceClubData(
      clubNumber: String,
      clubRows: Seq[TMClubDataPoint]
  ): Seq[TMClubDataPoint] = {

    val rowMap = clubRows.map(row => (row.programYear, row.month) -> row).toMap

    val sortedRows =
      clubRows.sortBy(row => (row.programYear, if (row.month >= 7) row.month else row.month + 12))

    val updatedRows = sortedRows.map { row =>
      if (row.month == 7) {
        row.monthlyGrowth = row.dcpData.newMembers + row.dcpData.addNewMembers
      } else {
        val prevMonth = rowMap.get(
          (row.programYear, (if (row.month == 1) 12 else row.month - 1))
        )
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
        val earlierVal =
          rowMap.get((row.programYear, 9)).map(_.activeMembers).getOrElse(-1)
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
        val earlierVal =
          rowMap.get((row.programYear, 3)).map(_.activeMembers).getOrElse(-1)
        row.members31Mar = earlierVal
      }

      row
    }
    updatedRows
  }

}
