package com.github.rorygraves.tm_data.data.district.historical

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df4dp
import slick.collection.heterogeneous._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length
import slick.sql.SqlProfile.ColumnOption.NotNull

import java.time.LocalDate
import scala.concurrent.Await

object DistrictSummaryHistoricalTableDef extends TableDef[DistrictSummaryHistoricalDataPoint] {

  val tableName = "district_summary_historical"

  private val districtColumnId     = "district"
  private val monthColumnId        = "program_month"
  private val asOfDateColumnId     = "as_of_date"
  private val programYearColumnId  = "program_year"
  private val monthEndDateColumnId = "month_end_date"

  import slick.jdbc.PostgresProfile.api._

  class DistrictSummaryHistoricalDataTable(tag: Tag) extends Table[DistrictSummaryHistoricalDataPoint](tag, tableName) {
    def monthEndDate                 = column[LocalDate](monthEndDateColumnId, NotNull)
    def asOfDate                     = column[LocalDate](asOfDateColumnId, NotNull)
    def month                        = column[Int](monthColumnId, NotNull)
    def programYear                  = column[Int](programYearColumnId, NotNull)
    def region                       = column[String]("region", Length(4), NotNull)
    def district                     = column[String]("district", Length(3), NotNull)
    def dsp                          = column[Boolean]("dsp", NotNull)
    def decTraining                  = column[Boolean]("dec_training", NotNull)
    def newPayments                  = column[Int]("new_payments", NotNull)
    def octPayments                  = column[Int]("oct_payments", NotNull)
    def aprilPayments                = column[Int]("april_payments", NotNull)
    def latePayments                 = column[Int]("late_payments", NotNull)
    def charterPayments              = column[Int]("charter_payments", NotNull)
    def totalYtdPayments             = column[Int]("total_ytd_payments", NotNull)
    def paymentBase                  = column[Int]("payment_base", NotNull)
    def percentPaymentGrowth         = column[Double]("percent_payment_growth", NotNull)
    def paidClubBase                 = column[Int]("paid_club_base", NotNull)
    def paidClubs                    = column[Int]("paid_clubs", NotNull)
    def percentClubGrowth            = column[Double]("percent_club_growth", NotNull)
    def activeClubs                  = column[Int]("active_clubs", NotNull)
    def distinguishedClubs           = column[Int]("distinguished_clubs", NotNull)
    def selectDistinguishedClubs     = column[Int]("select_distinguished_clubs", NotNull)
    def presidentsDistinguishedClubs = column[Int]("presidents_distinguished_clubs", NotNull)
    def totalDistinguishedClubs      = column[Int]("total_distinguished_clubs", NotNull)
    def percentDistinguishedClubs    = column[Double]("percent_distinguished_clubs", NotNull)
    def drpStatus                    = column[String]("drp_status", Length(3), NotNull)

    val pk   = primaryKey("pk__" + this.tableName, (district, monthEndDate))
    val idx1 = index("idx_" + this.tableName + "_dist_year_month", (programYear, district, month), unique = true)
    val idx2 = index("idx_" + this.tableName + "_year_month", (programYear, month), unique = false)

    def * = (
      month ::
        asOfDate ::
        monthEndDate ::
        programYear ::
        region ::
        district ::
        dsp ::
        decTraining ::
        newPayments ::
        octPayments ::
        aprilPayments ::
        latePayments ::
        charterPayments ::
        totalYtdPayments ::
        paymentBase ::
        percentPaymentGrowth ::
        paidClubBase ::
        paidClubs ::
        percentClubGrowth ::
        activeClubs ::
        distinguishedClubs ::
        selectDistinguishedClubs ::
        presidentsDistinguishedClubs ::
        totalDistinguishedClubs ::
        percentDistinguishedClubs ::
        drpStatus ::
        HNil
    ).mapTo[DistrictSummaryHistoricalDataPoint]
  }

  def insertOrUpdate(monthData: List[DistrictSummaryHistoricalDataPoint], dbRunner: DBRunner): Int = {

    val statments = monthData.map(tq.insertOrUpdate(_))
    val seq       = DBIO.sequence(statments).transactionally
    val res       = dbRunner.dbAwait(seq)
    res.sum
  }

  val tq = TableQuery[DistrictSummaryHistoricalDataTable]

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

  def allDistrictYearMonths(db: Database): Seq[(String, Int, Int, LocalDate)] = {

    val query = tq.map(t => (t.district, t.programYear, t.month, t.monthEndDate)).distinct
    Await.result(db.run(query.result), scala.concurrent.duration.Duration.Inf)
  }

  def districtStartDates(db: Database): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths(db)
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

  def existsByYearMonth(dbRunner: DBRunner, progYear: Int, month: Int): Boolean = {
    searchBy(dbRunner, None, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def searchBy(
      dbRunner: DBRunner,
      district: Option[String] = None,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[DistrictSummaryHistoricalDataPoint] = {

    val query =
      tq.filterOpt(district)(_.district === _).filterOpt(progYear)(_.programYear === _).filterOpt(month)(_.month === _)

    val queryWithLimit = limit match {
      case Some(l) => query.take(l)
      case None    => query
    }

    dbRunner.dbAwait(queryWithLimit.result).toList
  }

  def allDistrictIds(dbRunner: DBRunner): List[String] = {
    dbRunner.dbAwait(tq.map(_.district).distinct.result).toList
  }

//  override def indexes: List[IndexDef[DistrictSummaryHistoricalDataPoint]] = List(
//    IndexDef("_dist_year_month", this, List(programYearColumnId, monthColumnId, districtColumnId), unique = true),
//    IndexDef("_year_month", this, List(programYearColumnId, monthColumnId), unique = false)
//  )

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
