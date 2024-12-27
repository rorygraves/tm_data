package com.github.rorygraves.tm_data.data.area.perf

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import slick.jdbc.PostgresProfile.api._
import slick.relational.RelationalProfile.ColumnOption.Length

import java.time.LocalDate

class HistoricAreaPerfTable(val dbRunner: DBRunner) extends AbstractTable[TMAreaDataPoint] {

  override val tableName = "area_perf_historical"

  val columns: List[Column[TMAreaDataPoint]] = List(
    IntColumn("program_year", t => t.programYear),
    IntColumn("program_month", t => t.month),
    LocalDateColumn("month_end_date", t => t.monthEndDate),
    LocalDateColumn("as_of_date", t => t.asOfDate),
    StringColumn("district", t => t.district),
    StringColumn("division", t => t.division),
    StringColumn("area", t => t.area),
    IntColumn("club_base", t => t.paidClubBase),
    IntColumn("paid_clubs_req_for_dist", t => t.paidClubsReqForDist),
    IntColumn("paid_clubs_req_for_select", t => t.paidClubsReqForSelect),
    IntColumn("paid_clubs_req_for_pres", t => t.paidClubsReqForPres),
    IntColumn("cur_paid_clubs", t => t.curPaidClubs),
    IntColumn("dist_clubs_req_dist", t => t.distClubsReqDist),
    IntColumn("dist_clubs_req_select", t => t.distClubsReqSelect),
    IntColumn("dist_clubs_req_pres", t => t.distClubsReqPres),
    IntColumn("cur_dist_clubs", t => t.curDistClubs),
    StringColumn("distinguished_status", t => t.distinguishedStatus)
  )

  class TableDef(tag: Tag) extends Table[TMAreaDataPoint](tag, tableName) {
    val programYear           = column[Int]("program_year")
    val month                 = column[Int]("program_month")
    val monthEndDate          = column[LocalDate]("month_end_date")
    val asOfDate              = column[LocalDate]("as_of_date")
    val district              = column[String]("district", Length(3))
    val division              = column[String]("division", Length(2))
    val area                  = column[String]("area", Length(2))
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

    val pk   = primaryKey(s"pk_${this.tableName}", (programYear, district, month, division, area))
    val idx1 = index(s"idx_${this.tableName}_year_month", (programYear, month))
    val idx2 = index(s"idx_${this.tableName}_district", (district))
    val idx3 = index(s"idx_${this.tableName}_year_district_division", (programYear, district, division))
    val idx4 = index(s"idx_${this.tableName}_year_district_division_area", (programYear, district, division, area))

    // projection for TMDistClubDataPoint
    def * = (
      programYear,
      month,
      monthEndDate,
      asOfDate,
      district,
      division,
      area,
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
    ) <> (TMAreaDataPoint.tupled, TMAreaDataPoint.unapply)

  }

  val tq = TableQuery[TableDef]

  def latestAreaData(
      districtId: String,
      divisionId: String,
      area: String,
      year: Int
  ): Option[TMAreaDataPoint] = {
    latest(districtId, divisionId, Some(area), Some(year), Some(1)).headOption
  }

  def latest(
      districtId: String,
      divisionId: String,
      areaOpt: Option[String] = None,
      yearOpt: Option[Int] = None,
      take: Option[Int] = None
  ): List[TMAreaDataPoint] = {

    val base = tq
      .filter(r => r.district === districtId && r.division === divisionId)
      .filterOpt(areaOpt)((t, a) => t.area === a)
      .filterOpt(yearOpt)((t, y) => t.programYear === y)

    val last = base.map(_.monthEndDate).max

    val monthEnd = base.filter(r => r.monthEndDate === last)

    val query = take match {
      case Some(t) => monthEnd.take(t)
      case None    => monthEnd
    }
    dbRunner.dbAwait(query.result).toList
  }

  def areaDataByYear(districtId: String, divisionId: String, areaId: String, curYear: Int): List[TMAreaDataPoint] = {
    dbRunner
      .dbAwait(
        tq.filter(r =>
          r.district === districtId && r.division === divisionId && r.area === areaId && r.programYear === curYear
        ).result
      )
      .toList
      .sortBy(_.asOfDate)
  }

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    println("HistoricAreaPerfTable.createIfNotExists-------------------------------------------")
    createTableFromStatements(statements)
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
  ): List[TMAreaDataPoint] = {
    val baseQuery = tq
      .filter(_.district === district)
      .filterOpt(progYear)((t, y) => t.programYear === y)
      .filterOpt(month)((t, m) => t.month === m)

    val query = limit match {
      case Some(l) => baseQuery.take(l)
      case None    => baseQuery
    }

    dbRunner.dbAwait(query.result, "HistoricalAreaPerfTableDef.searchByDistrict").toList
  }

  def insertOrUpdate(monthData: List[TMAreaDataPoint]): Int = {

    val statements = monthData.map(tq.insertOrUpdate(_))
    val seq        = DBIO.sequence(statements).transactionally
    val res        = dbRunner.dbAwait(seq, "HistoricClubPerfTableDef.insertOrUpdate")
    res.sum
  }

  def getAreaData(
      district: String,
      yearOpt: Option[Int],
      monthOpt: Option[Int],
      divisionOpt: Option[String],
      areaIdOpt: Option[String]
  ): List[TMAreaDataPoint] = {
    val query = tq
      .filter(_.district === district)
      .filterOpt(yearOpt)((t, y) => t.programYear === y)
      .filterOpt(monthOpt)((t, m) => t.month === m)
      .filterOpt(divisionOpt)((t, d) => t.division === d)
      .filterOpt(areaIdOpt)((t, a) => t.area === a)
      .result

    dbRunner.dbAwait(query, "HistoricAreaPerfTableDef.getAreaData").toList
  }
}
