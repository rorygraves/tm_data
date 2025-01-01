package com.github.rorygraves.tm_data.data.district.historical

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df4dp
import slick.collection.heterogeneous._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length
import slick.sql.SqlProfile.ColumnOption.NotNull
import com.github.rorygraves.tm_data.util.CSVCodec.flexibleLocalDateCodec

import java.io.StringWriter
import java.time.LocalDate

class TMDataDistrictSummaryHistoricalTable(val dbRunner: DBRunner) extends AbstractTable[TMDistrictSummaryDataPoint] {

  val tableName = "district_summary_historical"

  private val monthColumnId        = "program_month"
  private val asOfDateColumnId     = "as_of_date"
  private val programYearColumnId  = "program_year"
  private val monthEndDateColumnId = "month_end_date"

  import slick.jdbc.PostgresProfile.api._

  class DistrictSummaryHistoricalDataTable(tag: Tag) extends Table[TMDistrictSummaryDataPoint](tag, tableName) {
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
    ).mapTo[TMDistrictSummaryDataPoint]
  }

  def allMonths = dbRunner.dbAwait(tq.map(r => (r.programYear, r.month)).distinct.sorted.result).toList

  def datesForDistrict(district: String) =
    dbRunner.dbAwait(tq.filter(_.district === district).map(r => (r.programYear, r.month, r.asOfDate)).result)

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    println("TMDataDistrictSummaryHistoricalTabel.createIfNotExists-------------------------------------------")
    createTableFromStatements(statements)
  }

  def insertOrUpdate(monthData: List[TMDistrictSummaryDataPoint]): Int = {

    val statements = monthData.map(tq.insertOrUpdate(_))
    dbRunner.dbAwait(DBIO.sequence(statements).transactionally).sum
  }

  val tq = TableQuery[DistrictSummaryHistoricalDataTable]

  /** returns first program year, month for this district */
  def firstOverviewDate(districtId: String): Option[(Int, Int)] = {
    val base = tq.filter(_.district === districtId)
    dbRunner
      .dbAwait(
        base.filter(_.monthEndDate === base.map(_.monthEndDate).min).take(1).map(r => (r.programYear, r.month)).result
      )
      .headOption
  }

  val columns: List[Column[TMDistrictSummaryDataPoint]] =
    List[Column[TMDistrictSummaryDataPoint]](
      LocalDateColumn(monthEndDateColumnId, t => t.monthEndDate),
      LocalDateColumn(asOfDateColumnId, t => t.asOfDate),
      IntColumn(monthColumnId, t => t.month),
      IntColumn(programYearColumnId, t => t.programYear),
      StringColumn("region", t => t.region),
      StringColumn("district", t => t.district),
      BooleanColumn("dsp", t => t.dsp),
      BooleanColumn("dec_training", t => t.decTraining),
      IntColumn("new_payments", t => t.newPayments),
      IntColumn("oct_payments", t => t.octPayments),
      IntColumn("april_payments", t => t.aprilPayments),
      IntColumn("late_payments", t => t.latePayments),
      IntColumn("charter_payments", t => t.charterPayments),
      IntColumn("total_ytd_payments", t => t.totalYtdPayments),
      IntColumn("payment_base", t => t.paymentBase),
      DoubleColumn("percent_payment_growth", t => t.percentPaymentGrowth, df4dp),
      IntColumn("paid_club_base", t => t.paidClubBase),
      IntColumn("paid_clubs", t => t.paidClubs),
      DoubleColumn("percent_club_growth", t => t.percentClubGrowth, df4dp),
      IntColumn("active_clubs", t => t.activeClubs),
      IntColumn("distinguished_clubs", t => t.distinguishedClubs),
      IntColumn("select_distinguished_clubs", t => t.selectDistinguishedClubs),
      IntColumn("presidents_distinguished_clubs", t => t.presidentsDistinguishedClubs),
      IntColumn("total_distinguished_clubs", t => t.totalDistinguishedClubs),
      DoubleColumn("percent_distinguished_clubs", t => t.percentDistinguishedClubs, df4dp),
      StringColumn("drp_status", t => t.drpStatus)
    )

  def allDistrictYearMonths(): Seq[(String, Int, Int, LocalDate)] = {
    dbRunner.dbAwait(tq.map(t => (t.district, t.programYear, t.month, t.monthEndDate)).distinct.result)
  }

  def districtStartDates(): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths()
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

  def existsByYearMonth(progYear: Int, month: Int): Boolean = {
    searchBy(None, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def searchBy(
      district: Option[String] = None,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[TMDistrictSummaryDataPoint] = {

    val query =
      tq.filterOpt(district)(_.district === _).filterOpt(progYear)(_.programYear === _).filterOpt(month)(_.month === _)

    val queryWithLimit = limit match {
      case Some(l) => query.take(l)
      case None    => query
    }

    dbRunner.dbAwait(queryWithLimit.result).toList
  }

  def allDistrictIds(): List[String] = {
    dbRunner.dbAwait(tq.map(_.district).distinct.result).toList
  }

  def districtData(
      districtId: String,
      yearOpt: Option[Int] = None,
      monthOpt: Option[Int] = None
  ): List[TMDistrictSummaryDataPoint] = {

    searchBy(Some(districtId), yearOpt, monthOpt).sortBy(_.monthEndDate)
  }

  def rowsToCSV(data: Seq[TMDistrictSummaryDataPoint]): String = {
    import kantan.csv._
    import kantan.csv.ops._
    import kantan.csv.generic._
    val out                                           = new StringWriter
    implicit val localDateCodec: CellCodec[LocalDate] = flexibleLocalDateCodec

    out.writeCsv(
      data,
      rfc.withHeader(
        "month",
        "as_of_date",
        "month_end_date",
        "program_year",
        "region",
        "district",
        "dsp",
        "dec_training",
        "new_payments",
        "oct_payments",
        "april_payments",
        "late_payments",
        "charter_payments",
        "total_ytd_payments",
        "payment_base",
        "percent_payment_growth",
        "paid_club_base",
        "paid_clubs",
        "percent_club_growth",
        "active_clubs",
        "distinguished_clubs",
        "select_distinguished_clubs",
        "presidents_distinguished_clubs",
        "total_distinguished_clubs",
        "percent_distinguished_clubs",
        "drp_status"
      )
    )
    out.toString

  }

  def getLatest(): Seq[TMDistrictSummaryDataPoint] = {

    val query = tq.filter(_.monthEndDate === tq.map(_.monthEndDate).max).result

    dbRunner.dbAwait(query).toList.sortBy(_.district)
  }

}
