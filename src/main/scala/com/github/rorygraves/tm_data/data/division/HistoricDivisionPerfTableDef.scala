package com.github.rorygraves.tm_data.data.division

import com.github.rorygraves.tm_data.util.DBRunner
import slick.jdbc.PostgresProfile.api._
import slick.relational.RelationalProfile.ColumnOption.Length

import java.time.LocalDate

class HistoricDivisionPerfTableDef(dbRunner: DBRunner) {

  val tableName = "division_perf_historical"

  class HistoricalDivisionPerfTable(tag: Tag) extends Table[TMDivisionDataPoint](tag, tableName) {
    val programYear           = column[Int]("program_year")
    val month                 = column[Int]("program_month")
    val monthEndDate          = column[LocalDate]("month_end_date")
    val asOfDate              = column[LocalDate]("as_of_date")
    val district              = column[String]("district", Length(3))
    val division              = column[String]("division", Length(2))
    val clubBase              = column[Int]("club_base")
    val paidClubsReqForDist   = column[Int]("paid_clubs_req_for_dist")
    val paidClubsReqForSelect = column[Int]("paid_clubs_req_for_select")
    val paidClubsReqForPres   = column[Int]("paid_clubs_req_for_pres")
    val curPaidClubs          = column[Int]("cur_paid_clubs")
    val distClubsReqDist      = column[Int]("dist_clubs_req_dist")
    val distClubsReqSelect    = column[Int]("dist_clubs_req_select")
    val distClubsReqPres      = column[Int]("dist_clubs_req_pres")
    val curDistClubs          = column[Int]("cur_dist_clubs")
    val distinguishedStatus   = column[String]("distinguished_status")

    val pk   = primaryKey(s"pk_${this.tableName}", (programYear, district, month, division))
    val idx1 = index(s"idx_${this.tableName}_year_month", (programYear, month))
    val idx2 = index(s"idx_${this.tableName}_district", district)

    // projection for TMDistClubDataPoint
    def * = (
      programYear,
      month,
      monthEndDate,
      asOfDate,
      district,
      division,
      clubBase,
      paidClubsReqForDist,
      paidClubsReqForSelect,
      paidClubsReqForPres,
      curPaidClubs,
      distClubsReqDist,
      distClubsReqSelect,
      distClubsReqPres,
      curDistClubs,
      distinguishedStatus
    ) <> (TMDivisionDataPoint.tupled, TMDivisionDataPoint.unapply)

  }

  val tq = TableQuery[HistoricalDivisionPerfTable]

  def districtDataByYear(districtId: String, curYear: Int): List[TMDivisionDataPoint] = {
    dbRunner
      .dbAwait(tq.filter(r => r.district === districtId && r.programYear === curYear).result)
      .toList
  }

  def createIfNotExists(): Unit = {
    dbRunner.dbAwait(tq.schema.createIfNotExists)
  }

  def latestDistrictMonthDates(): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths()
    allDistYearMonths
      .groupBy(_._1)
      .view
      .mapValues { distData =>
        val sorted = distData.sortBy(_._4)
        val last   = sorted.last
        (last._2, last._3)
      }
      .toMap

  }

  def existsByYearMonthDistrict(progYear: Int, month: Int, districtId: String): Boolean = {
    searchByDistrict(districtId, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def allDistrictYearMonths(): Seq[(String, Int, Int, LocalDate)] = {
    val query = tq.map(t => (t.district, t.programYear, t.month, t.monthEndDate)).distinct.result
    dbRunner.dbAwait(query).toList
  }

  def searchByDistrict(
      district: String,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[TMDivisionDataPoint] = {
    val baseQuery = tq
      .filter(_.district === district)
      .filterOpt(progYear)((t, y) => t.programYear === y)
      .filterOpt(month)((t, m) => t.month === m)

    val query = limit match {
      case Some(l) => baseQuery.take(l)
      case None    => baseQuery
    }

    dbRunner.dbAwait(query.result, "HistoricalDivisionPerfTableDef.searchByDistrict").toList
  }

  def insertOrUpdate(monthData: List[TMDivisionDataPoint]): Int = {

    val statements = monthData.map(tq.insertOrUpdate(_))
    val seq        = DBIO.sequence(statements).transactionally
    val res        = dbRunner.dbAwait(seq, "HistoricDivisionPerfTableDef.insertOrUpdate")
    res.sum
  }
}
