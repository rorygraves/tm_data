package com.github.rorygraves.tm_data.data.district.historical

import com.github.rorygraves.tm_data.db.{
  BooleanColumnDef,
  ColumnDef,
  Connection,
  DataSource,
  DoubleColumnDef,
  IndexDef,
  IntColumnDef,
  LocalDateColumnDef,
  Search,
  SearchItem,
  StringColumnDef,
  TableDef
}
import com.github.rorygraves.tm_data.util.FormatUtil.df4dp

import java.sql.ResultSet
import java.time.LocalDate

object DistrictSummaryHistoricalTableDef extends TableDef[DistrictSummaryHistoricalDataPoint] {

  val tableName = "district_summary_historical"

  private val districtColumnId     = "district"
  private val monthColumnId        = "program_month"
  private val asOfDateColumnId     = "as_of_date"
  private val programYearColumnId  = "program_year"
  private val monthEndDateColumnId = "month_end_date"

  val columns: List[ColumnDef[DistrictSummaryHistoricalDataPoint]] =
    List[ColumnDef[DistrictSummaryHistoricalDataPoint]](
      LocalDateColumnDef(monthEndDateColumnId, t => t.monthEndDate, primaryKey = true),
      LocalDateColumnDef(asOfDateColumnId, t => t.asOfDate),
      IntColumnDef(monthColumnId, t => t.month),
      IntColumnDef(programYearColumnId, t => t.programYear),
      StringColumnDef("region", t => t.region, length = 4),
      StringColumnDef("district", t => t.district, primaryKey = true, length = 3),
      BooleanColumnDef("dsp", t => t.dsp),
      BooleanColumnDef("dec_training", t => t.decTraining),
      IntColumnDef("new_payments", t => t.newPayments),
      IntColumnDef("oct_payments", t => t.octPayments),
      IntColumnDef("april_payments", t => t.aprilPayments),
      IntColumnDef("late_payments", t => t.latePayments),
      IntColumnDef("charter_payments", t => t.charterPayments),
      IntColumnDef("total_ytd_payments", t => t.totalYtdPayments),
      IntColumnDef("payment_base", t => t.paymentBase),
      DoubleColumnDef("percent_payment_growth", t => t.percentPaymentGrowth, df4dp),
      IntColumnDef("paid_club_base", t => t.paidClubBase),
      IntColumnDef("paid_clubs", t => t.paidClubs),
      DoubleColumnDef("percent_club_growth", t => t.percentClubGrowth, df4dp),
      IntColumnDef("active_clubs", t => t.activeClubs),
      IntColumnDef("distinguished_clubs", t => t.distinguishedClubs),
      IntColumnDef("select_distinguished_clubs", t => t.selectDistinguishedClubs),
      IntColumnDef("presidents_distinguished_clubs", t => t.presidentsDistinguishedClubs),
      IntColumnDef("total_distinguished_clubs", t => t.totalDistinguishedClubs),
      DoubleColumnDef("percent_distinguished_clubs", t => t.percentDistinguishedClubs, df4dp),
      StringColumnDef("drp_status", t => t.drpStatus, length = 3)
    )

  def allDistrictYearMonths(ds: DataSource): Seq[(String, Int, Int, LocalDate)] = {
    val listBuilder = List.newBuilder[(String, Int, Int, LocalDate)]
    ds.run(implicit conn => {
      conn.executeQuery(
        s"SELECT DISTINCT $districtColumnId, $programYearColumnId, $monthColumnId, $monthEndDateColumnId FROM $tableName",
        rs => {
          while (rs.next()) {
            listBuilder += ((rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getDate(4).toLocalDate))
          }
        }
      )
    })
    listBuilder.result()
  }

  def districtStartDates(ds: DataSource): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths(ds)
    allDistYearMonths
      .groupBy(_._1)
      .view
      .mapValues { distData =>
        val sorted = distData.sortBy(_._4)
        val first  = sorted.head
        (first._2, first._3)
      }
      .toMap
  }

  def existsByYearMonth(conn: Connection, progYear: Int, month: Int): Boolean = {
    searchBy(conn, None, Some(progYear), Some(month), Some(1)).nonEmpty
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
      conn: Connection,
      district: Option[String] = None,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[DistrictSummaryHistoricalDataPoint] = {
    val searchKey = HDSearchKey(district, progYear, month)
    val search    = ValueSearch(searchKey)
    conn.search(search, limit)
  }

  def allDistrictIds(dataSource: DataSource): List[String] = {
    val listBuilder = List.newBuilder[String]
    dataSource.run(implicit conn => {
      conn.executeQuery(
        s"SELECT DISTINCT $districtColumnId FROM $tableName",
        rs => {
          while (rs.next()) {
            listBuilder += rs.getString(1)
          }
        }
      )
    })
    listBuilder.result()
  }

  override def indexes: List[IndexDef[DistrictSummaryHistoricalDataPoint]] = List(
    IndexDef("_dist_year_month", this, List(programYearColumnId, monthColumnId, districtColumnId), unique = true),
    IndexDef("_year_month", this, List(programYearColumnId, monthColumnId), unique = false)
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
      rs.getString("region"),
      rs.getString("district"),
      rs.getBoolean("dsp"),
      rs.getBoolean("dec_training"),
      rs.getInt("new_payments"),
      rs.getInt("oct_payments"),
      rs.getInt("april_payments"),
      rs.getInt("late_payments"),
      rs.getInt("charter_payments"),
      rs.getInt("total_ytd_payments"),
      rs.getInt("payment_base"),
      rs.getDouble("percent_payment_growth"),
      rs.getInt("paid_club_base"),
      rs.getInt("paid_clubs"),
      rs.getDouble("percent_club_growth"),
      rs.getInt("active_clubs"),
      rs.getInt("distinguished_clubs"),
      rs.getInt("select_distinguished_clubs"),
      rs.getInt("presidents_distinguished_clubs"),
      rs.getInt("total_distinguished_clubs"),
      rs.getDouble("percent_distinguished_clubs"),
      rs.getString("drp_status")
    )
  }
}
