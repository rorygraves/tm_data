package com.github.rorygraves.tm_data.data.district.historical

import com.github.rorygraves.tm_data.util.TMUtil

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
      else str.dropRight(1).toDouble / 100
    }

    val monthEndDate = TMUtil.computeMonthEndDate(programYear, month)

    val dsp                       = row("DSP") == "Y"
    val decTraining               = row("Training") == "Y"
    val paidClubs                 = row("Paid Clubs").toInt
    val paidClubBase              = row("Paid Club Base").toInt
    val percentDistinguishedClubs = percentageParser(row("% Distinguished Clubs"))
    val percentClubGrowth         = percentageParser(row("% Club Growth"))
    val percentPaymentGrowth      = percentageParser(row("% Payment Growth"))

    val drpStatus = if (dsp && decTraining) {
      if (percentDistinguishedClubs >= 0.55 && percentClubGrowth >= 0.05 && percentPaymentGrowth >= 0.08) "SM"
      else if (percentDistinguishedClubs >= 0.50 && percentClubGrowth >= 0.03 && percentPaymentGrowth >= 0.05) "P"
      else if (percentDistinguishedClubs >= 0.45 && paidClubs - paidClubBase >= 1 && percentPaymentGrowth >= 0.03) "S"
      else if (percentDistinguishedClubs >= 0.40 && paidClubs - paidClubBase >= 0 && percentPaymentGrowth >= 0.01) "D"
      else ""
    } else ""

    DistrictSummaryHistoricalDataPoint(
      month,
      asOfDate,
      monthEndDate,
      programYear,
      region = row("REGION"),
      district = row("DISTRICT"),
      dsp = dsp,
      decTraining = decTraining,
      newPayments = row("New Payments").toInt,
      octPayments = row("October Payments").toInt,
      aprilPayments = row("April Payments").toInt,
      latePayments = row("Late Payments").toInt,
      charterPayments = row("Charter Payments").toInt,
      totalYtdPayments = row("Total YTD Payments").toInt,
      paymentBase = row("Payment Base").toInt,
      percentPaymentGrowth = percentClubGrowth,
      paidClubBase = paidClubBase,
      paidClubs = paidClubs,
      percentClubGrowth = percentClubGrowth,
      activeClubs = row("Active Clubs").toInt,
      distinguishedClubs = row("Distinguished Clubs").toInt,
      selectDistinguishedClubs = row("Select Distinguished Clubs").toInt,
      presidentsDistinguishedClubs = row("Presidents Distinguished Clubs").toInt,
      totalDistinguishedClubs = row("Total Distinguished Clubs").toInt,
      percentDistinguishedClubs = percentDistinguishedClubs,
      drpStatus = drpStatus
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
    decTraining: Boolean,
    newPayments: Int,
    octPayments: Int,
    aprilPayments: Int,
    latePayments: Int,
    charterPayments: Int,
    totalYtdPayments: Int,
    paymentBase: Int,
    percentPaymentGrowth: Double,
    paidClubBase: Int,
    paidClubs: Int,
    percentClubGrowth: Double,
    activeClubs: Int,
    distinguishedClubs: Int,
    selectDistinguishedClubs: Int,
    presidentsDistinguishedClubs: Int,
    totalDistinguishedClubs: Int,
    percentDistinguishedClubs: Double,
    drpStatus: String
) extends Ordered[DistrictSummaryHistoricalDataPoint] {

  private def sortKey = (programYear, asOfDate, month, district)

  override def compare(that: DistrictSummaryHistoricalDataPoint): Int = this.sortKey compare that.sortKey
}
