package app.data.district.historical

import app.Main.{df2dp, df4dp}
import app.db
import app.db._

import java.sql.ResultSet

object DistrictSummaryHistoricalTableDef extends TableDef[DistrictSummaryHistoricalDataPoint] {

  val tableName = "District_Summary_Historical"

  private val districtColumnId     = "District"
  private val monthColumnId        = "Month"
  private val asOfDateColumnId     = "AsOfDate"
  private val programYearColumnId  = "ProgramYear"
  private val monthEndDateColumnId = "MonthEndDate"

  val columns: List[ColumnDef[DistrictSummaryHistoricalDataPoint]] =
    List[ColumnDef[DistrictSummaryHistoricalDataPoint]](
      LocalDateColumnDef(monthEndDateColumnId, t => t.monthEndDate, primaryKey = true),
      LocalDateColumnDef(asOfDateColumnId, t => t.asOfDate),
      IntColumnDef(monthColumnId, t => t.month),
      IntColumnDef(programYearColumnId, t => t.programYear),
      StringColumnDef("Region", t => t.region, length = 4),
      StringColumnDef("District", t => t.district, primaryKey = true, length = 3),
      BooleanColumnDef("DSP", t => t.dsp),
      BooleanColumnDef("DECTraining", t => t.decTraining),
      IntColumnDef("NewPayments", t => t.newPayments),
      IntColumnDef("OctPayments", t => t.octPayments),
      IntColumnDef("AprilPayments", t => t.aprilPayments),
      IntColumnDef("LatePayments", t => t.latePayments),
      IntColumnDef("CharterPayments", t => t.charterPayments),
      IntColumnDef("TotalYtdPayments", t => t.totalYtdPayments),
      IntColumnDef("PaymentBase", t => t.paymentBase),
      DoubleColumnDef("PercentPaymentGrowth", t => t.paymentGrowth, df4dp),
      IntColumnDef("PaidClubBase", t => t.paidClubBase),
      IntColumnDef("PaidClubs", t => t.paidClubs),
      DoubleColumnDef("PercentClubGrowth", t => t.clubGrowth, df4dp),
      IntColumnDef("ActiveClubs", t => t.activeClubs),
      IntColumnDef("DistinguishedClubs", t => t.distinguishedClubs),
      IntColumnDef("SelectDistinguishedClubs", t => t.selectDistinguishedClubs),
      IntColumnDef("PresidentsDistinguishedClubs", t => t.presidentsDistinguishedClubs),
      IntColumnDef("TotalDistinguishedClubs", t => t.totalDistinguishedClubs),
      DoubleColumnDef("PercentDistinguishedClubs", t => t.distinguishedClubsPercentage, df4dp)

    )

  def existsByYearMonth(dataSource: DataSource, progYear: Int, month: Int): Boolean = {
    searchBy(dataSource, None, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  case class HDSearchKey(
      district: Option[String],
      programYear: Option[Int],
      month: Option[Int]
  ) {
    def searchItems: List[SearchItem] = {
      List(
        district.map(d => SearchItem(districtColumnId, (stmt, idx) => stmt.setString(idx, d))),
        programYear.map(py => SearchItem(programYearColumnId, (stmt, idx) => stmt.setInt(idx, py))),
        month.map(m => SearchItem(monthColumnId, (stmt, idx) => stmt.setInt(idx, m)))
      ).flatten
    }
  }

  private case class ValueSearch(searchKey: HDSearchKey) extends Search[DistrictSummaryHistoricalDataPoint] {
    override def tableName: String = DistrictSummaryHistoricalTableDef.tableName

    override def searchItems: List[SearchItem] = searchKey.searchItems

    override def columns: Option[List[String]] = None

    override def reader: ResultSet => DistrictSummaryHistoricalDataPoint = read
  }

  def searchBy(
      dataSource: DataSource,
      district: Option[String] = None,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[DistrictSummaryHistoricalDataPoint] = {
    val searchKey = HDSearchKey(district, progYear, month)
    val search    = ValueSearch(searchKey)
    dataSource.run(implicit conn => {
      conn.search(search, limit)
    })
  }

  override def indexes: List[IndexDef[DistrictSummaryHistoricalDataPoint]] = List(
    db.IndexDef("_dist_year_month", this, List(programYearColumnId, monthColumnId, districtColumnId), unique = true),
    db.IndexDef("_year_month", this, List(programYearColumnId, monthColumnId), unique = false)
  )

  def read(rs: java.sql.ResultSet): DistrictSummaryHistoricalDataPoint = {
    val programYear  = rs.getInt(programYearColumnId)
    val month        = rs.getInt(monthColumnId)
    val asOfDate     = rs.getDate(asOfDateColumnId).toLocalDate
    val monthEndDate = rs.getDate(monthEndDateColumnId).toLocalDate

    DistrictSummaryHistoricalDataPoint(
      month,
      asOfDate,
      monthEndDate,
      programYear,
      rs.getString("Region"),
      rs.getString("District"),
      rs.getBoolean("DSP"),
      rs.getBoolean("DECTraining"),
      rs.getInt("NewPayments"),
      rs.getInt("OctPayments"),
      rs.getInt("AprilPayments"),
      rs.getInt("LatePayments"),
      rs.getInt("CharterPayments"),
      rs.getInt("TotalYtdPayments"),
      rs.getInt("PaymentBase"),
      rs.getDouble("PercentPaymentGrowth"),
      rs.getInt("PaidClubBase"),
      rs.getInt("PaidClubs"),
      rs.getDouble("PercentClubGrowth"),
      rs.getInt("ActiveClubs"),
      rs.getInt("DistinguishedClubs"),
      rs.getInt("SelectDistinguishedClubs"),
      rs.getInt("PresidentsDistinguishedClubs"),
      rs.getInt("TotalDistinguishedClubs"),
      rs.getDouble("PercentDistinguishedClubs")
    )
  }
}
