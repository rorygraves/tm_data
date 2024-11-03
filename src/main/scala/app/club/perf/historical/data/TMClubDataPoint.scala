package app.club.perf.historical.data

import java.time.LocalDate

object TMClubDataPoint {
  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String],
      clubDivDataPoints: Map[ClubMatchKey, TMDivClubDataPoint],
      clubDistDataPoints: Map[ClubMatchKey, TMDistClubDataPoint]
  ): TMClubDataPoint = {

    val clubNumber = data("Club Number")
    val dataKey    = ClubMatchKey(programYear, month, clubNumber)
    TMClubDataPoint(
      programYear,
      month,
      asOfDate,
      data("District"),
      data("Division"),
      data("Area"),
      clubNumber,
      data("Club Name"),
      data("Club Status"),
      data("Mem. Base").toInt,
      data("Active Members").toInt,
      data("Goals Met").toInt,
      data("Mem. dues on time Apr") == "1",
      data("Mem. dues on time Oct") == "1",
      data("Club Distinguished Status"),
      ClubDCPData.fromDistrictClubReportCSV(programYear, data),
      clubDivDataPoints.get(dataKey),
      clubDistDataPoints.get(dataKey)
    )
  }
}

case class TMClubDataPoint(
    programYear: Int,
    month: Int,
    asOfDate: LocalDate,
    district: String,
    division: String,
    area: String,
    clubNumber: String,
    clubName: String,
    clubStatus: String,
    memBase: Int,
    activeMembers: Int,
    goalsMet: Int,
    memDuesOnTimeApr: Boolean,
    memDuesOnTimeOct: Boolean,
    clubDistinctiveStatus: String,
    dcpData: ClubDCPData,
    divData: Option[TMDivClubDataPoint],
    distData: Option[TMDistClubDataPoint]
) extends Ordered[TMClubDataPoint] {

  def key = s"$monthEndDate-$clubNumber"

  def monthEndDate: LocalDate = {
    if (month >= 7)
      LocalDate.of(programYear, month, 1).plusMonths(1).minusDays(1)
    else // next year
      LocalDate.of(programYear + 1, month, 1).plusMonths(1).minusDays(1)
  }

  lazy val cotMet: Boolean =
    dcpData.officersTrainedRd1 > 3 && dcpData.officersTrainedRd2 > 3

  lazy val goal10Met: Boolean =
    (memDuesOnTimeApr || memDuesOnTimeOct) && dcpData.officerListOnTime

  lazy val membershipGrowth: Int = activeMembers - memBase

  lazy val awardsPerMember: Double =
    if (activeMembers > 0) dcpData.totalAwards.toDouble / activeMembers else 0.0

  lazy val dcpEligibility: Boolean =
    activeMembers > 19 || membershipGrowth > 2

  var monthlyGrowth: Int = 0
  var members30Sept: Int = 0
  var members31Mar: Int  = 0

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
