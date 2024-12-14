package com.github.rorygraves.tm_data.data.club.perf.historical

import com.github.rorygraves.tm_data.{DocumentType, TMDocumentDownloader}
import com.github.rorygraves.tm_data.TMDocumentDownloader.reportDownloader
import com.github.rorygraves.tm_data.data.club.perf.historical.data.{
  ClubDCPData,
  ClubMatchKey,
  HistoricClubPerfTableDef,
  TMClubDataPoint,
  TMDistClubDataPoint,
  TMDivClubDataPoint
}
import com.github.rorygraves.tm_data.data.district.historical.TMDataDistrictSummaryHistoricalTable
import com.github.rorygraves.tm_data.util.TMUtil
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.slf4j.LoggerFactory

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDate
import scala.jdk.CollectionConverters.IterableHasAsJava

/** Class to generate club data from the TI club reports */
class HistoricClubPerfGenerator(
                                 districtSummaryHistoricalTableDef: TMDataDistrictSummaryHistoricalTable,
                                 historicClubPerfTableDef: HistoricClubPerfTableDef
) {

  private val logger = LoggerFactory.getLogger(getClass)

  import slick.jdbc.PostgresProfile.api._
  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      region: String,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint]
  ): TMClubDataPoint = {

    try {
      val clubNumber = data
        .getOrElse(
          "Club Number",
          data.getOrElse(
            "Club",
            throw new IllegalStateException(s"Field not found 'ClubNumber' in ${data.keys.mkString(",")}")
          )
        )
        .toInt
      val district = data("District")

      def findPrev(programYear: Int, month: Int): Option[TMClubDataPoint] = {
        historicClubPerfTableDef.findByClubYearMonth(clubNumber, programYear, month)
      }

      val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)

      val dataKey = ClubMatchKey(programYear, month, clubNumber)

      def parseInt(s: String): Int = {
        if (s.isEmpty) 0 else s.toInt
      }

      val memBase               = parseInt(data("Mem. Base"))
      val activeMembers: Int    = data("Active Members").toInt
      val membershipGrowth: Int = activeMembers - memBase

      val dcpData = ClubDCPData.fromDistrictClubReportCSV(programYear, month, asOfDate, clubNumber, data)

      def computeMonthlyGrowth(): Int = {
        if (month == 7) {
          dcpData.newMembers + dcpData.addNewMembers
        } else {
          val prevCount = findPrev(programYear, if (month == 1) 12 else month - 1) match {
            case Some(prev) =>
              prev.dcpData.newMembers + prev.dcpData.addNewMembers
            case None =>
              println(
                s"Warning! No previous month found for growth for Year: $programYear Club: $clubNumber Month: $month"
              )
              0
          }
          dcpData.newMembers + dcpData.addNewMembers - prevCount
        }
      }

      val members30Sept =
        if (month == 7 || month == 8 || month == 9)
          activeMembers
        else
          findPrev(programYear, 9).map(_.activeMembers).getOrElse(0)

      val members31Mar =
        if (month == 7 || month == 8 || month == 9)
          0
        else if (month == 10 || month == 11 || month == 12 || month == 1 || month == 2 || month == 3)
          activeMembers
        else
          findPrev(programYear, 3).map(_.activeMembers).getOrElse(0)

      val monthlyGrowth = computeMonthlyGrowth()

      val awardsPerMember: Double =
        if (activeMembers > 0 && dcpData.totalAwards > 0) dcpData.totalAwards.toDouble / activeMembers else 0.0

      val dcpEligibility: Boolean =
        activeMembers > 19 || membershipGrowth > 2

      TMClubDataPoint(
        programYear,
        month,
        monthEndDate,
        asOfDate,
        district,
        region,
        data("Division"),
        data("Area"),
        clubNumber,
        data("Club Name"),
        data("Club Status"),
        memBase,
        activeMembers,
        membershipGrowth,
        awardsPerMember,
        dcpEligibility,
        data("Goals Met").toInt,
        dcpData,
        data("Club Distinguished Status"),
        monthlyGrowth,
        members30Sept,
        members31Mar,
        clubDivDataPoints.get(dataKey).map(_.toClubData).getOrElse(TMDivClubDataPoint.empty),
        clubDistDataPoints(dataKey).toClubData
      )
    } catch {
      case e: Exception => {
        println(s"Error processing club data: ${e.getMessage}")
        println(s"Data: ${data.mkString(" ")}")
        throw e
      }
    }
  }
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

  /** Import historic month end data from TI.  Returns the number of rows in the last month of data processed */
  def generateHistoricalClubData(
      cacheFolder: String,
      districtId: String,
      progStartYear: Int,
      progStartMonth: Int
  ): Int = {

    logger.info(s"generateHistoricalClubData($districtId)")
    val startYear = progStartYear
    val endYear   = TMUtil.currentProgramYear

    var lastMonthCount = 0
    (startYear to endYear).foreach { progYear =>
      println(f"Running historical club data import for year District $districtId-$progYear")
      val startMonthOpt = if (progYear == progStartYear) Some(progStartMonth) else None
      lastMonthCount = downloadHistoricalClubData(progYear, districtId, startMonthOpt, cacheFolder)
    }

    outputClubData(districtId)

    lastMonthCount
  }

  def monthExists(progYear: Int, month: Int, districtId: String): Boolean = {
    historicClubPerfTableDef.existsByYearMonthDistrict(progYear, month, districtId)
  }

  val allProgramMonths = List(7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6)

  def downloadHistoricalClubData(
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

        val divData  = generateHistoricalDivData(progYear, month, districtId, cacheFolder)
        val distData = generateHistoricalClubDistData(progYear, month, districtId, cacheFolder)

        val clubDivDataPoints  = divData.map(r => r.matchKey -> r).toMap
        val clubDistDataPoints = distData.map(r => r.matchKey -> r).toMap

        val regionOpt: Option[String] = {

          def findRegionByProgYearMonth(progYear: Int, month: Int): Option[String] = {
            districtSummaryHistoricalTableDef
              .searchBy(Some(districtId), Some(progYear), Some(month), limit = Some(1))
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
            fromDistrictClubReportCSV(
              year,
              month,
              region,
              asOfDate,
              rawData,
              clubDivDataPoints,
              clubDistDataPoints
            )
          }
        )

        if (monthData.nonEmpty) {
          val monthDataCleaned = cleanMonthData(monthData)
          logger.info(s"  storing D$districtId $progYear-$month - rows: ${monthData.length}  ")
          if (monthDataCleaned.length != monthData.length) {
            logger.info(s"  WARNING - rows removed - cleaned rows: ${monthDataCleaned.length}")
          }

          if (monthDataCleaned.nonEmpty) {
            val count = historicClubPerfTableDef.insertOrUpdate(monthDataCleaned)
            logger.info(s"  result $count - rows inserted/updated")
            lastMonthCount = count
          }
        }
      }
    }
    lastMonthCount
  }

  private def cleanMonthData(monthData: List[TMClubDataPoint]): List[TMClubDataPoint] = {
    monthData.groupBy(_.clubNumber).values.map(_.last).toList
  }

  def outputClubData(districtId: String): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      val data = historicClubPerfTableDef.searchByDistrict(districtId).sorted

      // output the headers
      printer.printRecord(historicClubPerfTableDef.columns.map(_.name).asJava)
      // output the rows
      data.foreach { tmClubPoint =>
        val rowValues = historicClubPerfTableDef.columns.map(_.csvExportFn(tmClubPoint))
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
