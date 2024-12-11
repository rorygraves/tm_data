package com.github.rorygraves.tm_data.data.club.perf.historical

import com.github.rorygraves.tm_data.{DocumentType, TMDocumentDownloader}
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.data.club.perf.historical.data.{
  HistoricClubPerfTableDef,
  TMClubDataPoint,
  TMDistClubDataPoint,
  TMDivClubDataPoint
}
import com.github.rorygraves.tm_data.data.district.historical.DistrictSummaryHistoricalTableDef

import com.github.rorygraves.tm_data.util.{DBRunner, TMUtil}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile.api._

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import scala.jdk.CollectionConverters.IterableHasAsJava

/** Class to generate club data from the TI club reports */
object HistoricClubPerfGenerator {

  private val logger = LoggerFactory.getLogger(getClass)

  def generateHistoricalDivData(
      progYear: Int,
      month: Int,
      districtId: String,
      cacheFolder: String
  ): List[TMDivClubDataPoint] = {
    logger.info(s"generateHistoricalDivData($progYear, $month, $districtId)")
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
      districtId: String,
      cacheFolder: String
  ): List[TMDistClubDataPoint] = {
    logger.info(s"generateHistoricalClubDistData($progYear, $month, $districtId)")
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

  def generateHistoricalClubData(
      cacheFolder: String,
      districtId: String,
      progStartYear: Int,
      progStartMonth: Int,
      dbRunner: DBRunner
  ): Unit = {

    logger.info(s"generateHistoricalClubData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    (startYear to endYear).foreach { progYear =>
      println(f"Running historical club data import for year District $districtId-$progYear")
      val startMonthOpt = if (progYear == progStartYear) Some(progStartMonth) else None
      downloadHistoricalClubData(progYear, districtId, startMonthOpt, cacheFolder, dbRunner)
    }

    outputClubData(dbRunner, districtId)
  }

  def monthExists(progYear: Int, month: Int, districtId: String, dbRunner: DBRunner): Boolean = {
    HistoricClubPerfTableDef.existsByYearMonthDistrict(dbRunner, progYear, month, districtId)
  }

  val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

  def downloadHistoricalClubData(
      progYear: Int,
      districtId: String,
      startMonthOpt: Option[Int],
      cacheFolder: String,
      dbRunner: DBRunner
  ): Unit = {

    // iterate over the months in the year first fetch months 7-12 then 1-6 to align with the TM year (July to June)

    val months = startMonthOpt match {
      case Some(startMonth) =>
        allProgramMonths.dropWhile(_ != startMonth)
      case None =>
        allProgramMonths
    }

    months.foreach { month =>
      val targetMonth        = TMUtil.programMonthToSOMDate(progYear, month)
      val isRecent           = targetMonth.isAfter(LocalDate.now().minusMonths(2))
      val monthAlreadyExists = monthExists(progYear, month, districtId, dbRunner)
      if (monthAlreadyExists && !isRecent) {
        logger.info(f"Skipping District $districtId $progYear $month for year $progYear - already exists")
      } else if (targetMonth.isAfter(LocalDate.now())) {
        logger.info(f"Skipping month District $districtId $progYear $month  - it is in the future")
      } else {
        logger.info(s"Processing District $districtId $progYear-$month ($targetMonth)")

        val divData  = generateHistoricalDivData(progYear, month, districtId, cacheFolder)
        val distData = generateHistoricalClubDistData(progYear, month, districtId, cacheFolder)

        val clubDivDataPoints  = divData.map(r => r.matchKey -> r).toMap
        val clubDistDataPoints = distData.map(r => r.matchKey -> r).toMap

        val regionOpt: Option[String] = {

          def findRegionByProgYearMonth(progYear: Int, month: Int): Option[String] = {
            DistrictSummaryHistoricalTableDef
              .searchBy(dbRunner, Some(districtId), Some(progYear), Some(month), limit = Some(1))
              .headOption
              .map(_.region)
          }

          findRegionByProgYearMonth(progYear, month) match {
            case Some(region) => Some(region)
            case None =>
              val (y2, m2) =
                if (month == 7) (progYear - 1, 6) else if (month == 1) (progYear, 12) else (progYear, month - 1)
              findRegionByProgYearMonth(y2, m2)
          }
        }

        lazy val region = regionOpt.getOrElse(
          throw new IllegalStateException(s"Unable to infer region for district $districtId $progYear $month")
        )

        logger.info(s"  Downloading club data for D$districtId $progYear-$month")
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
              region,
              asOfDate,
              rawData,
              clubDivDataPoints,
              clubDistDataPoints,
              dbRunner
            )
          }
        )

        val monthDataCleaned = cleanMonthData(monthData)
        logger.info(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")
        if (monthDataCleaned.length != monthData.length) {
          logger.info(s"  WARNING - rows removed - cleaned rows: ${monthDataCleaned.length}")
        }

        if (monthDataCleaned.nonEmpty) {
          val count = HistoricClubPerfTableDef.insertOrUpdate(monthDataCleaned, dbRunner)
          logger.info(s"  result $count - rows inserted/updated")
        }
        logger.info(s"  processing complete-------------------------------------------------------------")
      }
    }
  }

  private def cleanMonthData(monthData: List[TMClubDataPoint]): List[TMClubDataPoint] = {
    monthData.groupBy(_.clubNumber).values.map(_.last).toList
  }

  def outputClubData(dbRunner: DBRunner, districtId: String): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      val data = HistoricClubPerfTableDef.searchByDistrict(dbRunner, districtId).sorted

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
