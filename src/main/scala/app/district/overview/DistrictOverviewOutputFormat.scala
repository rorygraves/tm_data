package app.district.overview

object DistrictOverviewOutputFormat {

  case class ColumnDef(
    name: String,
    calculation: DistrictOverviewDataPoint => String
  )

  val columnGenerator: List[ColumnDef] = List[ColumnDef](
    ColumnDef("REGION", t => t.region),
    ColumnDef("DISTRICT", t => t.district),
    ColumnDef("DSP", t => if (t.dsp) "Y" else "N"),
    ColumnDef("Training", t => if (t.training) "Y" else "N"),
    ColumnDef("New Payments", t => t.newPayments.toString),
    ColumnDef("April Payments", t => t.aprilPayments.toString),
    ColumnDef("October Payments", t => t.octoberPayments.toString),
    ColumnDef("Late Payments", t => t.latePayments.toString),
    ColumnDef("Charter Payments", t => t.charterPayments.toString),
    ColumnDef("Total YTD Payments", t => t.totalYtdPayments.toString),
    ColumnDef("Payment Base", t => t.paymentBase.toString),
    ColumnDef("% Payment Growth", t => t.paymentGrowth),
    ColumnDef("Paid Club Base", t => t.paidClubBase.toString),
    ColumnDef("Paid Clubs", t => t.paidClubs.toString),
    ColumnDef("% Club Growth", t => t.clubGrowth),
    ColumnDef("Active Clubs", t => t.activeClubs.toString),
    ColumnDef("Distinguished Clubs", t => t.distinguishedClubs.toString),
    ColumnDef("Select Distinguished Clubs", t => t.selectDistinguishedClubs.toString),
    ColumnDef("Presidents Distinguished Clubs", t => t.presidentsDistinguishedClubs.toString),
    ColumnDef("Total Distinguished Clubs", t => t.totalDistinguishedClubs.toString),
    ColumnDef("% Distinguished Clubs", t => t.distinguishedClubsPercentage)
  )
}