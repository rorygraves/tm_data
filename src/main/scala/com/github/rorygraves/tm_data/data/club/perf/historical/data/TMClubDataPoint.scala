package com.github.rorygraves.tm_data.data.club.perf.historical.data

import app.db.Connection
import com.github.rorygraves.tm_data.util.TMUtil

import java.time.LocalDate

object TMClubDataPoint {

  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      region: String,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint],
      conn: Connection
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

      def findPrev(programYear: Int, month: Int, conn: Connection): Option[TMClubDataPoint] = {
        HistoricClubPerfTableDef.findByClubYearMonth(clubNumber, programYear, month, conn)
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
          val prevCount = findPrev(programYear, if (month == 1) 12 else month - 1, conn) match {
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
          findPrev(programYear, 9, conn).map(_.activeMembers).getOrElse(0)

      val members31Mar =
        if (month == 7 || month == 8 || month == 9)
          0
        else if (month == 10 || month == 11 || month == 12 || month == 1 || month == 2 || month == 3)
          activeMembers
        else
          findPrev(programYear, 3, conn).map(_.activeMembers).getOrElse(0)

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
        clubDivDataPoints.get(dataKey),
        clubDistDataPoints.get(dataKey)
      )
    } catch {
      case e: Exception => {
        println(s"Error processing club data: ${e.getMessage}")
        println(s"Data: ${data.mkString(" ")}")
        throw e
      }
    }
  }
}

case class TMClubDataPoint(
    programYear: Int,
    month: Int,
    monthEndDate: LocalDate,
    asOfDate: LocalDate,
    district: String,
    region: String,
    division: String,
    area: String,
    clubNumber: Int,
    clubName: String,
    clubStatus: String,
    memBase: Int,
    activeMembers: Int,
    membershipGrowth: Int,
    awardsPerMember: Double,
    dcpEligibility: Boolean,
    goalsMet: Int,
    dcpData: ClubDCPData,
    clubDistinctiveStatus: String,
    monthlyGrowth: Int,
    members30Sept: Int,
    members31Mar: Int,
    divData: Option[TMDivClubDataPoint],
    distData: Option[TMDistClubDataPoint]
) extends Ordered[TMClubDataPoint] {

  override def compare(that: TMClubDataPoint): Int = {

    // compare by year, by month (order 7-12,1-6), asOfDate, clubNumber
    val yearCompare = programYear.compareTo(that.programYear)
    if (yearCompare != 0) {
      return yearCompare
    }
    val monthCompare       = if (month >= 7) month else month + 12
    val thatMonthCompare   = if (that.month >= 7) that.month else that.month + 12
    val monthCompareResult = monthCompare.compareTo(thatMonthCompare)
    if (monthCompareResult != 0) {
      return monthCompareResult
    }
    val asOfDateCompare = asOfDate.compareTo(that.asOfDate)
    if (asOfDateCompare != 0) {
      return asOfDateCompare
    }
    clubNumber.compareTo(that.clubNumber)

  }
}
