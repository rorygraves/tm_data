package app.data.district.historical

import app.Main.df2dp
import app.db
import app.db._

import java.sql.ResultSet

object HistoricDistrictPerfTableDef extends TableDef[DistrictOverviewDataPoint] {

  val tableName = "District_Perf_Historical"

  private val keyColumnId          = "Key"
  private val districtColumnId     = "District"
  private val monthColumnId        = "Month"
  private val asOfDateColumnId     = "AsOfDate"
  private val programYearColumnId  = "ProgramYear"
  private val clubNumberColumnId   = "ClubNumber"
  private val monthEndDateColumnId = "MonthEndDate"

  val columns: List[ColumnDef[DistrictOverviewDataPoint]] = List[ColumnDef[DistrictOverviewDataPoint]](
    IntColumnDef(monthColumnId, t => t.month, primaryKey = true),
    LocalDateColumnDef(asOfDateColumnId, t => t.asOfDate),
    LocalDateColumnDef(monthEndDateColumnId, t => t.monthEndDate),
    IntColumnDef(programYearColumnId, t => t.programYear, primaryKey = true),
    StringColumnDef("Region", t => t.region),
    StringColumnDef("District", t => t.district, primaryKey = true),
    BooleanColumnDef("DSP", t => t.dsp),
    BooleanColumnDef("Training", t => t.training),
    IntColumnDef("NewPayments", t => t.newPayments),
    IntColumnDef("AprilPayments", t => t.aprilPayments),
    IntColumnDef("OctoberPayments", t => t.octoberPayments),
    IntColumnDef("LatePayments", t => t.latePayments),
    IntColumnDef("CharterPayments", t => t.charterPayments),
    IntColumnDef("TotalYtdPayments", t => t.totalYtdPayments),
    IntColumnDef("PaymentBase", t => t.paymentBase),
    DoubleColumnDef("PaymentGrowth", t => t.paymentGrowth, df2dp),
    IntColumnDef("PaidClubBase", t => t.paidClubBase),
    IntColumnDef("PaidClubs", t => t.paidClubs),
    DoubleColumnDef("ClubGrowth", t => t.clubGrowth, df2dp),
    IntColumnDef("ActiveClubs", t => t.activeClubs),
    IntColumnDef("DistinguishedClubs", t => t.distinguishedClubs),
    IntColumnDef("SelectDistinguishedClubs", t => t.selectDistinguishedClubs),
    IntColumnDef("PresidentsDistinguishedClubs", t => t.presidentsDistinguishedClubs),
    IntColumnDef("TotalDistinguishedClubs", t => t.totalDistinguishedClubs),
    DoubleColumnDef("DistinguishedClubsPercentage", t => t.distinguishedClubsPercentage, df2dp)
  )

  def existsByYearMonth(dataSource: DataSource, progYear: Int, month: Int): Boolean = {
    searchBy(dataSource, None, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  case class HDSearchKey(
      key: Option[String],
      district: Option[String],
      programYear: Option[Int],
      month: Option[Int]
  ) {
    def searchItems: List[SearchItem] = {
      List(
        key.map(k => SearchItem(keyColumnId, (stmt, idx) => stmt.setString(idx, k))),
        district.map(d => SearchItem(districtColumnId, (stmt, idx) => stmt.setString(idx, d))),
        programYear.map(py => SearchItem(programYearColumnId, (stmt, idx) => stmt.setInt(idx, py))),
        month.map(m => SearchItem(monthColumnId, (stmt, idx) => stmt.setInt(idx, m)))
      ).flatten
    }
  }

  private case class ValueSearch(searchKey: HDSearchKey) extends Search[DistrictOverviewDataPoint] {
    override def tableName: String = HistoricDistrictPerfTableDef.tableName

    override def searchItems: List[SearchItem] = searchKey.searchItems

    override def columns: Option[List[String]] = None

    override def reader: ResultSet => DistrictOverviewDataPoint = read
  }

  def searchBy(
      dataSource: DataSource,
      district: Option[String] = None,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[DistrictOverviewDataPoint] = {
    val searchKey = HDSearchKey(None, district, progYear, month)
    val search    = ValueSearch(searchKey)
    dataSource.run(implicit conn => {
      conn.search(search, limit)
    })
  }

  override def indexes: List[IndexDef[DistrictOverviewDataPoint]] = List(
    db.IndexDef("_dist_year_month", this, List(programYearColumnId, monthColumnId, districtColumnId), unique = true),
    db.IndexDef("_year_month", this, List(programYearColumnId, monthColumnId), unique = false)
  )

  def read(rs: java.sql.ResultSet): DistrictOverviewDataPoint = {
    val programYear  = rs.getInt(programYearColumnId)
    val month        = rs.getInt(monthColumnId)
    val asOfDate     = rs.getDate(asOfDateColumnId).toLocalDate
    val monthEndDate = rs.getDate(monthEndDateColumnId).toLocalDate

    DistrictOverviewDataPoint(
      month,
      asOfDate,
      monthEndDate,
      programYear,
      rs.getString("Region"),
      rs.getString("District"),
      rs.getBoolean("DSP"),
      rs.getBoolean("Training"),
      rs.getInt("NewPayments"),
      rs.getInt("AprilPayments"),
      rs.getInt("OctoberPayments"),
      rs.getInt("LatePayments"),
      rs.getInt("CharterPayments"),
      rs.getInt("TotalYtdPayments"),
      rs.getInt("PaymentBase"),
      rs.getDouble("PaymentGrowth"),
      rs.getInt("PaidClubBase"),
      rs.getInt("PaidClubs"),
      rs.getDouble("ClubGrowth"),
      rs.getInt("ActiveClubs"),
      rs.getInt("DistinguishedClubs"),
      rs.getInt("SelectDistinguishedClubs"),
      rs.getInt("PresidentsDistinguishedClubs"),
      rs.getInt("TotalDistinguishedClubs"),
      rs.getDouble("DistinguishedClubsPercentage")
    )
  }
}
