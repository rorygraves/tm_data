package app.data.club.perf.historical.data

import app.data.district.historical.{DistrictOverviewDataPoint, HistoricDistrictPerfTableDef}
import app.db.DataSource
import app.util.TMUtil

import java.time.LocalDate

object TMClubDataPoint {

  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint],
      dataSource: DataSource
  ): TMClubDataPoint = {

    val clubNumber = data("Club Number")
    val district   = data("District")
    val region = HistoricDistrictPerfTableDef
      .searchBy(dataSource, Some(district), Some(programYear), Some(month), limit = Some(1))
      .headOption
      .map(_.region)
      .getOrElse("UNKNOWN")

    def findPrev(programYear: Int, month: Int): Option[TMClubDataPoint] = {
      HistoricClubPerfTableDef.findByClubYearMonth(dataSource, clubNumber, programYear, month)
    }

    val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)
    def key          = s"$monthEndDate-$clubNumber"

    val dataKey = ClubMatchKey(programYear, month, clubNumber)

    val memBase               = data("Mem. Base").toInt
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
              s"Warning! No previous month found for growth for Year: ${programYear} Club: $clubNumber Month: ${month}"
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
        findPrev(programYear, 9).map(_.activeMembers).getOrElse(-1)

    val members31Mar =
      if (month == 7 || month == 8 || month == 9)
        0
      else if (month == 10 || month == 11 || month == 12 || month == 1 || month == 2 || month == 3)
        activeMembers
      else
        findPrev(programYear, 3).map(_.activeMembers).getOrElse(-1)

    val monthlyGrowth = computeMonthlyGrowth()

    val awardsPerMember: Double =
      if (activeMembers > 0 && dcpData.totalAwards > 0) dcpData.totalAwards.toDouble / activeMembers else 0.0

    val dcpEligibility: Boolean =
      activeMembers > 19 || membershipGrowth > 2

    TMClubDataPoint(
      key,
      month,
      asOfDate,
      monthEndDate,
      programYear,
      district,
      region,
      data("Division"),
      data("Area"),
      clubNumber,
      data("Club Name"),
      data("Club Status"),
      membershipGrowth,
      awardsPerMember,
      dcpEligibility,
      memBase,
      activeMembers,
      data("Goals Met").toInt,
      dcpData,
      data("Club Distinguished Status"),
      monthlyGrowth,
      members30Sept,
      members31Mar,
      clubDivDataPoints.get(dataKey),
      clubDistDataPoints.get(dataKey)
    )
  }
}

case class TMClubDataPoint(
    key: String,
    month: Int,
    asOfDate: LocalDate,
    monthEndDate: LocalDate,
    programYear: Int,
    district: String,
    region: String,
    division: String,
    area: String,
    clubNumber: String,
    clubName: String,
    clubStatus: String,
    membershipGrowth: Int,
    awardsPerMember: Double,
    dcpEligibility: Boolean,
    memBase: Int,
    activeMembers: Int,
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
