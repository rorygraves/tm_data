package app.data.district.historical

import app.util.TMUtil

import java.time.LocalDate

object DistrictOverviewDataPoint {
  def fromOverviewReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      row: Map[String, String]
  ): DistrictOverviewDataPoint = {

    val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)
    DistrictOverviewDataPoint(
      month,
      asOfDate,
      monthEndDate,
      programYear,
      region = row("REGION"),
      district = row("DISTRICT"),
      dsp = row("DSP") == "Y",
      training = row("Training") == "Y",
      newPayments = row("New Payments").toInt,
      aprilPayments = row("April Payments").toInt,
      octoberPayments = row("October Payments").toInt,
      latePayments = row("Late Payments").toInt,
      charterPayments = row("Charter Payments").toInt,
      totalYtdPayments = row("Total YTD Payments").toInt,
      paymentBase = row("Payment Base").toInt,
      paymentGrowth = row("% Payment Growth"),
      paidClubBase = row("Paid Club Base").toInt,
      paidClubs = row("Paid Clubs").toInt,
      clubGrowth = row("% Club Growth"),
      activeClubs = row("Active Clubs").toInt,
      distinguishedClubs = row("Distinguished Clubs").toInt,
      selectDistinguishedClubs = row("Select Distinguished Clubs").toInt,
      presidentsDistinguishedClubs = row("Presidents Distinguished Clubs").toInt,
      totalDistinguishedClubs = row("Total Distinguished Clubs").toInt,
      distinguishedClubsPercentage = row("% Distinguished Clubs")
    )
  }
}

case class DistrictOverviewDataPoint(
    month: Int,
    asOfDate: LocalDate,
    monthEndDate: LocalDate,
    programYear: Int,
    region: String,
    district: String,
    dsp: Boolean,
    training: Boolean,
    newPayments: Int,
    aprilPayments: Int,
    octoberPayments: Int,
    latePayments: Int,
    charterPayments: Int,
    totalYtdPayments: Int,
    paymentBase: Int,
    paymentGrowth: String,
    paidClubBase: Int,
    paidClubs: Int,
    clubGrowth: String,
    activeClubs: Int,
    distinguishedClubs: Int,
    selectDistinguishedClubs: Int,
    presidentsDistinguishedClubs: Int,
    totalDistinguishedClubs: Int,
    distinguishedClubsPercentage: String
)
