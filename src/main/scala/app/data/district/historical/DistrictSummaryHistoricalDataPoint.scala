package app.data.district.historical

import app.util.TMUtil

import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

object DistrictSummaryHistoricalDataPoint {
  def fromOverviewReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      row: Map[String, String]
  ): DistrictSummaryHistoricalDataPoint = {

    def percentageParser(str: String): Double = {
      if (str == "N/A") 0.0
      else str.dropRight(1).toDouble
    }

    val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)
    DistrictSummaryHistoricalDataPoint(
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
      paymentGrowth = percentageParser(row("% Payment Growth")),
      paidClubBase = row("Paid Club Base").toInt,
      paidClubs = row("Paid Clubs").toInt,
      clubGrowth = percentageParser(row("% Club Growth")),
      activeClubs = row("Active Clubs").toInt,
      distinguishedClubs = row("Distinguished Clubs").toInt,
      selectDistinguishedClubs = row("Select Distinguished Clubs").toInt,
      presidentsDistinguishedClubs = row("Presidents Distinguished Clubs").toInt,
      totalDistinguishedClubs = row("Total Distinguished Clubs").toInt,
      distinguishedClubsPercentage = percentageParser(row("% Distinguished Clubs"))
    )
  }
}

case class DistrictSummaryHistoricalDataPoint(
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
    paymentGrowth: Double,
    paidClubBase: Int,
    paidClubs: Int,
    clubGrowth: Double,
    activeClubs: Int,
    distinguishedClubs: Int,
    selectDistinguishedClubs: Int,
    presidentsDistinguishedClubs: Int,
    totalDistinguishedClubs: Int,
    distinguishedClubsPercentage: Double
) extends Ordered[DistrictSummaryHistoricalDataPoint] {

  private def sortKey = (programYear, asOfDate, month, district)

  override def compare(that: DistrictSummaryHistoricalDataPoint): Int = this.sortKey compare that.sortKey
}
