package app.club.perf.historical.data

import java.time.LocalDate

object TMClubDataPoint {

  private def computeMonthEndDate(programYear: Int, month: Int): LocalDate = {
    if (month >= 7)
      LocalDate.of(programYear, month, 1).plusMonths(1).minusDays(1)
    else // next year
      LocalDate.of(programYear + 1, month, 1).plusMonths(1).minusDays(1)
  }

  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint]
  ): TMClubDataPoint = {

    val clubNumber = data("Club Number")

    val monthEndDate = computeMonthEndDate(programYear, month)
    def key          = s"$monthEndDate-$clubNumber"

    val dataKey = ClubMatchKey(programYear, month, clubNumber)

    val memBase               = data("Mem. Base").toInt
    val activeMembers: Int    = data("Active Members").toInt
    val membershipGrowth: Int = activeMembers - memBase

    val dcpData = ClubDCPData.fromDistrictClubReportCSV(programYear, month, asOfDate, clubNumber, data)
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
      data("District"),
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
      0, // TODO compute this from previous month in place
      0, // TODO compute this from previous month in place
      0, // TODO compute this from previous month in place
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
    var monthlyGrowth: Int,
    var members30Sept: Int,
    var members31Mar: Int,
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
