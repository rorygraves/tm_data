package app.data.club.perf.historical.data

import java.time.LocalDate

object TMDivClubDataPoint {
  def fromDivisionReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String]
  ): TMDivClubDataPoint = {
    TMDivClubDataPoint(
      programYear,
      month,
      asOfDate,
      data("District"),
      data("Division"),
      data("Area"),
      data("Club"), // Club Number
      data("Club Name"),
      data("October Renewals").toInt,
      data("April Renewals").toInt,
      data("Nov Visit award").toInt > 0,
      data("May Visit award").toInt > 0,
      data("Membership to date").toInt,
      data("Club Goals Met").toInt,
      data("Distinguished Club")
    )
  }
}

case class TMDivClubDataPoint(
    programYear: Int,
    month: Int,
    asOfDate: LocalDate,
    district: String,
    division: String,
    area: String,
    clubNumber: String,
    clubStatus: String,
    octRenewals: Int,
    aprRenewals: Int,
    novADVisit: Boolean,
    mayADVisit: Boolean,
    activeMembers: Int,
    goalsMet: Int,
    distinguishedStatus: String
) {

  def matchKey: ClubMatchKey = ClubMatchKey(programYear, month, clubNumber)

}
