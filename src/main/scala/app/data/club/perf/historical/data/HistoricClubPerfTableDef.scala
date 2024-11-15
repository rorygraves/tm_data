package app.data.club.perf.historical.data

import app.Main.df2dp
import app.db
import app.db.{
  BooleanColumnDef,
  ColumnDef,
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

import java.sql.ResultSet
import java.time.LocalDate

object HistoricClubPerfTableDef extends TableDef[TMClubDataPoint] {

  val tableName = "Club_Perf_Historical"

  private val keyColumnId          = "Key"
  private val districtColumnId     = "District"
  private val monthColumnId        = "Month"
  private val asOfDateColumnId     = "AsOfDate"
  private val programYearColumnId  = "ProgramYear"
  private val clubNumberColumnId   = "ClubNumber"
  private val monthEndDateColumnId = "MonthEndDate"

  val columns: List[ColumnDef[TMClubDataPoint]] = List[ColumnDef[TMClubDataPoint]](
    IntColumnDef(monthColumnId, t => t.month),
    LocalDateColumnDef(asOfDateColumnId, t => t.asOfDate),
    LocalDateColumnDef(monthEndDateColumnId, t => t.monthEndDate, primaryKey = true),
    IntColumnDef(programYearColumnId, t => t.programYear),
    StringColumnDef("District", t => t.district, length = 3),
    StringColumnDef("Division", t => t.division, length = 2),
    StringColumnDef("Area", t => t.area),
    IntColumnDef(clubNumberColumnId, t => t.clubNumber, primaryKey = true),
    StringColumnDef("ClubName", t => t.clubName),
    StringColumnDef("ClubStatus", t => t.clubStatus, length = 10),
    IntColumnDef("BaseMembers", t => t.memBase),
    IntColumnDef("ActiveMembers", t => t.activeMembers),
    IntColumnDef("GoalsMet", t => t.goalsMet),
    // dcp data
    IntColumnDef("CCs", t => t.dcpData.oldCCs),
    IntColumnDef("CCsAdd", t => t.dcpData.oldCCsAdd),
    IntColumnDef("ACs", t => t.dcpData.oldACs),
    IntColumnDef("ACsAdd", t => t.dcpData.oldACsAdd),
    IntColumnDef("Ldr", t => t.dcpData.oldLeaders),
    IntColumnDef("LdrsAdd", t => t.dcpData.oldLeadersAdd),
    IntColumnDef("Level1s", t => t.dcpData.p1Level1s),
    IntColumnDef("Level2s", t => t.dcpData.p2Level2s),
    IntColumnDef("Level2sAdd", t => t.dcpData.p3Level2sAdd),
    IntColumnDef("Level3s", t => t.dcpData.p4Level3s),
    IntColumnDef("Level45DTMs", t => t.dcpData.p5Level45D),
    IntColumnDef("Level45DTMsAdd", t => t.dcpData.p6Level45DAdd),
    IntColumnDef("NewMembers", t => t.dcpData.newMembers),
    IntColumnDef("AddNewMembers", t => t.dcpData.addNewMembers),
    IntColumnDef("OfficersTrainedRd1", t => t.dcpData.officersTrainedRd1),
    IntColumnDef("OfficersTrainedRd2", t => t.dcpData.officersTrainedRd2),
    BooleanColumnDef("COTMet", t => t.dcpData.cotMet),
    BooleanColumnDef("MembersDuesOnTimeOct", t => t.dcpData.memDuesOnTimeOct),
    BooleanColumnDef("MembersDuesOnTimeApr", t => t.dcpData.memDuesOnTimeApr),
    BooleanColumnDef("OfficerListOnTime", t => t.dcpData.officerListOnTime),
    BooleanColumnDef("Goal10Met", t => t.dcpData.goal10Met),
    StringColumnDef("DistinguishedStatus", t => t.clubDistinctiveStatus, length = 2),
    IntColumnDef("MembersGrowth", t => t.membershipGrowth),
    DoubleColumnDef("AwardsPerMember", t => t.awardsPerMember, df2dp),
    BooleanColumnDef("DCPEligibility", t => t.dcpEligibility),
    IntColumnDef("MonthlyGrowth", t => t.monthlyGrowth),
    IntColumnDef("30SeptMembers", t => t.members30Sept),
    IntColumnDef("31MarMembers", t => t.members31Mar),
    StringColumnDef("Region", t => t.region, length = 4),
    BooleanColumnDef("NovADVisit", t => t.divData.exists(_.novADVisit)),
    BooleanColumnDef("MayADVisit", t => t.divData.exists(_.mayADVisit)),
    // from district report
    IntColumnDef("TotalNewMembers", t => t.distData.map(_.totalNewMembers).getOrElse(0)),
    IntColumnDef("LateRenewals", t => t.distData.map(_.lateRenewals).getOrElse(0)),
    IntColumnDef("OctRenewals", t => t.distData.map(_.octRenewals).getOrElse(0)),
    IntColumnDef("AprRenewals", t => t.distData.map(_.aprRenewals).getOrElse(0)),
    IntColumnDef("TotalCharter", t => t.distData.map(_.totalCharter).getOrElse(0)),
    IntColumnDef("TotalToDate", t => t.distData.map(_.totalToDate).getOrElse(0)),
    StringColumnDef("CharterSuspendDate", t => t.distData.map(_.charterSuspendDate).getOrElse(""), length = 50)
  )

  def existsByYearMonthDistrict(dataSource: DataSource, progYear: Int, month: Int, districtId: Int): Boolean = {
    searchByDistrict(dataSource, districtId.toString, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def readClubDCPData(
      set: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubId: Int
  ): ClubDCPData = {
    ClubDCPData(
      programYear,
      month,
      asOfDate,
      clubId,
      set.getInt("CCs"),
      set.getInt("CCsAdd"),
      set.getInt("ACs"),
      set.getInt("ACsAdd"),
      set.getInt("Ldr"),
      set.getInt("LdrsAdd"),
      set.getInt("Level1s"),
      set.getInt("Level2s"),
      set.getInt("Level2sAdd"),
      set.getInt("Level3s"),
      set.getInt("Level45DTMs"),
      set.getInt("Level45DTMsAdd"),
      set.getBoolean("OfficerListOnTime"),
      set.getBoolean("Goal10Met"),
      set.getInt("OfficersTrainedRd1"),
      set.getInt("OfficersTrainedRd2"),
      set.getBoolean("COTMet"),
      set.getBoolean("MembersDuesOnTimeApr"),
      set.getBoolean("MembersDuesOnTimeOct"),
      set.getInt("NewMembers"),
      set.getInt("AddNewMembers")
    )
  }

  case class HDSearchKey(
      key: Option[String],
      district: Option[String],
      programYear: Option[Int],
      month: Option[Int],
      clubNumber: Option[Int]
  ) {
    def searchItems: List[SearchItem] = {
      List(
        key.map(k => SearchItem(keyColumnId, (stmt, idx) => stmt.setString(idx, k))),
        district.map(d => SearchItem(districtColumnId, (stmt, idx) => stmt.setString(idx, d))),
        programYear.map(py => SearchItem(programYearColumnId, (stmt, idx) => stmt.setInt(idx, py))),
        month.map(m => SearchItem(monthColumnId, (stmt, idx) => stmt.setInt(idx, m))),
        clubNumber.map(cn => SearchItem("ClubNumber", (stmt, idx) => stmt.setInt(idx, cn)))
      ).flatten
    }
  }

  private case class ValueSearch(searchKey: HDSearchKey) extends Search[TMClubDataPoint] {
    override def tableName: String             = HistoricClubPerfTableDef.tableName
    override def searchItems: List[SearchItem] = searchKey.searchItems
    override def columns: Option[List[String]] = None

    override def reader: ResultSet => TMClubDataPoint = read
  }

  def searchByDistrict(
      dataSource: DataSource,
      district: String,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[TMClubDataPoint] = {
    val searchKey = HDSearchKey(None, Some(district), progYear, month, None)
    val search    = ValueSearch(searchKey)
    dataSource.run(implicit conn => {
      conn.search(search, limit)
    })
  }

  override def indexes: List[IndexDef[TMClubDataPoint]] = List(
    db.IndexDef("_club_year_month", this, List(programYearColumnId, monthColumnId, clubNumberColumnId), unique = true)
  )

  def findByClubYearMonth(
      dataSource: DataSource,
      clubNumber: Int,
      programYear: Int,
      month: Int
  ): Option[TMClubDataPoint] = {
    val searchKey = HDSearchKey(None, None, Some(programYear), Some(month), Some(clubNumber))
    val search    = ValueSearch(searchKey)
    dataSource.run(implicit conn => {
      conn.search(search, limit = Some(1)).headOption
    })
  }

  def read(rs: java.sql.ResultSet): TMClubDataPoint = {
    val programYear  = rs.getInt(programYearColumnId)
    val month        = rs.getInt(monthColumnId)
    val asOfDate     = rs.getDate(asOfDateColumnId).toLocalDate
    val monthEndDate = rs.getDate(monthEndDateColumnId).toLocalDate
    val clubNumber   = rs.getInt("ClubNumber")

    TMClubDataPoint(
      month,
      asOfDate,
      monthEndDate,
      programYear,
      rs.getString("District"),
      rs.getString("Region"),
      rs.getString("Division"),
      rs.getString("Area"),
      clubNumber,
      rs.getString("ClubName"),
      rs.getString("ClubStatus"),
      rs.getInt("MembersGrowth"),
      rs.getDouble("AwardsPerMember"),
      rs.getBoolean("DCPEligibility"),
      rs.getInt("BaseMembers"),
      rs.getInt("ActiveMembers"),
      rs.getInt("GoalsMet"),
      readClubDCPData(rs, programYear, month, asOfDate, clubNumber),
      rs.getString("DistinguishedStatus"),
      rs.getInt("MonthlyGrowth"),
      rs.getInt("30SeptMembers"),
      rs.getInt("31MarMembers"),
      Some(readTMDivData(rs, programYear, month, asOfDate, clubNumber)),
      Some(readTMDistData(rs, programYear, month, asOfDate, clubNumber))
    )
  }

  private def readTMDistData(
      rs: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: Int
  ): TMDistClubDataPoint = {
    TMDistClubDataPoint(
      programYear,
      month,
      asOfDate,
      rs.getString("District"),
      rs.getString("Division"),
      rs.getString("Area"),
      clubNumber,
      rs.getString("ClubName"),
      rs.getInt("TotalNewMembers"),
      rs.getInt("LateRenewals"),
      rs.getInt("OctRenewals"),
      rs.getInt("AprRenewals"),
      rs.getInt("TotalCharter"),
      rs.getInt("TotalToDate"),
      rs.getString("CharterSuspendDate")
    )
  }

  private def readTMDivData(
      rs: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: Int
  ): TMDivClubDataPoint = {
    TMDivClubDataPoint(
      programYear,
      month,
      asOfDate,
      rs.getString("District"),
      rs.getString("Division"),
      rs.getString("Area"),
      clubNumber,
      rs.getString("ClubStatus"),
      rs.getInt("OctRenewals"),
      rs.getInt("AprRenewals"),
      rs.getBoolean("NovADVisit"),
      rs.getBoolean("MayADVisit"),
      rs.getInt("ActiveMembers"),
      rs.getInt("GoalsMet"),
      rs.getString("DistinguishedStatus")
    )
  }
}
