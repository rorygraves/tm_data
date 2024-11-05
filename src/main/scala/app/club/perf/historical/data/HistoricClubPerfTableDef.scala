package app.club.perf.historical.data

import app.Main.df2dp
import app.data._

import java.sql.ResultSet
import java.time.LocalDate

object HistoricClubPerfTableDef extends TableDef[TMClubDataPoint] {

  val tableName = "Club_Perf_Historical"

  val columns: List[ColumnDef[TMClubDataPoint]] = List[ColumnDef[TMClubDataPoint]](
    StringColumnDef[TMClubDataPoint]("Key", t => t.key, primaryKey = true),
    IntColumnDef("Month", t => t.month),
    LocalDateColumnDef("AsOfDate", t => t.asOfDate),
    LocalDateColumnDef("MonthEndDate", t => t.monthEndDate),
    IntColumnDef("ProgramYear", t => t.programYear),
    StringColumnDef("District", t => t.district),
    StringColumnDef("Division", t => t.division),
    StringColumnDef("Area", t => t.area),
    StringColumnDef("ClubNumber", t => t.clubNumber),
    StringColumnDef("ClubName", t => t.clubName),
    StringColumnDef("ClubStatus", t => t.clubStatus),
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
    StringColumnDef("DistinguishedStatus", t => t.clubDistinctiveStatus),
    IntColumnDef("MembersGrowth", t => t.membershipGrowth),
    DoubleColumnDef("AwardsPerMember", t => t.awardsPerMember, df2dp),
    BooleanColumnDef("DCPEligibility", t => t.dcpEligibility),
    IntColumnDef("MonthlyGrowth", t => t.monthlyGrowth),
    IntColumnDef("30SeptMembers", t => t.members30Sept),
    IntColumnDef("31MarMembers", t => t.members31Mar),
    StringColumnDef("Region", _ => "XXX"),
    BooleanColumnDef("NovADVisit", t => t.divData.exists(_.novADVisit)),
    BooleanColumnDef("MayADVisit", t => t.divData.exists(_.mayADVisit)),
    // from district report
    IntColumnDef("TotalNewMembers", t => t.distData.map(_.totalNewMembers).getOrElse(0)),
    IntColumnDef("LateRenewals", t => t.distData.map(_.lateRenewals).getOrElse(0)),
    IntColumnDef("OctRenewals", t => t.distData.map(_.octRenewals).getOrElse(0)),
    IntColumnDef("AprRenewals", t => t.distData.map(_.aprRenewals).getOrElse(0)),
    IntColumnDef("TotalCharter", t => t.distData.map(_.totalCharter).getOrElse(0)),
    IntColumnDef("TotalToDate", t => t.distData.map(_.totalToDate).getOrElse(0)),
    StringColumnDef("CharterSuspendDate", t => t.distData.map(_.charterSuspendDate).getOrElse(""))
  )

  def readClubDCBData(
      set: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubId: String
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

  def read(rs: java.sql.ResultSet): TMClubDataPoint = {
    val programYear  = rs.getInt("ProgramYear")
    val month        = rs.getInt("Month")
    val asOfDate     = rs.getDate("AsOfDate").toLocalDate
    val monthEndDate = rs.getDate("MonthEndDate").toLocalDate
    val clubNumber   = rs.getString("ClubNumber")

    TMClubDataPoint(
      rs.getString("Key"),
      month,
      asOfDate,
      monthEndDate,
      programYear,
      rs.getString("District"),
      rs.getString("Division"),
      rs.getString("Area"),
      clubNumber,
      rs.getString("ClubName"),
      rs.getString("ClubStatus"),
      rs.getInt("BaseMembers"),
      rs.getInt("ActiveMembers"),
      rs.getInt("GoalsMet"),
      readClubDCBData(rs, programYear, month, asOfDate, clubNumber),
      rs.getString("DistinguishedStatus"),
      Some(readTMDivData(rs, programYear, month, asOfDate, clubNumber)),
      Some(readTMDistData(rs, programYear, month, asOfDate, clubNumber))
    )
  }

  private def readTMDistData(
      rs: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: String
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
      clubNumber: String
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