package com.github.rorygraves.tm_data.data.club.perf.historical.data

import java.time.LocalDate

object TMDistClubDataPoint {
  def fromDistrictReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String]
  ): TMDistClubDataPoint = {
    TMDistClubDataPoint(
      programYear,
      month,
      asOfDate,
      data("District"),
      data("Division"),
      data("Area"),
      data("Club").toInt, // Club Number
      data("Club Name"),
      data("New").toInt,
      data("Late Ren.").toInt,
      data("Oct. Ren.").toInt,
      data("Apr. Ren.").toInt,
      data("Total Ren.").toInt,
      data("Total to Date").toInt,
      data("Charter Date/Suspend Date")
    )
  }
}

case class TMDistClubDataPoint(
    programYear: Int,
    month: Int,
    asOfDate: LocalDate,
    district: String,
    division: String,
    area: String,
    clubNumber: Int,
    clubName: String,
    totalNewMembers: Int,
    lateRenewals: Int,
    octRenewals: Int,
    aprRenewals: Int,
    totalCharter: Int,
    totalToDate: Int,
    charterSuspendDate: String
) {

  def matchKey: ClubMatchKey = ClubMatchKey(programYear, month, clubNumber)

}
