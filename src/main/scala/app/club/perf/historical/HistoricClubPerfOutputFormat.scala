package app.club.perf.historical

import app.Main.df2dp
import app.club.perf.historical.data.TMClubDataPoint

object HistoricClubPerfOutputFormat {

  case class ColumnDef(
      name: String,
      calculation: TMClubDataPoint => String
  )

  val clubColumnGenerator: List[ColumnDef] = List[ColumnDef](
    ColumnDef("Key", t => t.key),
    ColumnDef("Month", t => t.month.toString),
    ColumnDef("AsOfDate", t => t.asOfDate.toString),
    ColumnDef("MonthEndDate", t => t.monthEndDate.toString),
    ColumnDef("ProgramYear", t => t.programYear.toString),
    ColumnDef("District", t => t.district),
    ColumnDef("Division", t => t.division),
    ColumnDef("Area", t => t.area),
    ColumnDef("ClubNumber", t => t.clubNumber),
    ColumnDef("ClubName", t => t.clubName),
    ColumnDef("ClubStatus", t => t.clubStatus),
    ColumnDef("BaseMembers", t => t.memBase.toString),
    ColumnDef("ActiveMembers", t => t.activeMembers.toString),
    ColumnDef("GoalsMet", t => t.goalsMet.toString),
    // dcp data
    ColumnDef("CCs", t => t.dcpData.oldCCs.toString),
    ColumnDef("CCsAdd", t => t.dcpData.oldCCsAdd.toString),
    ColumnDef("ACs", t => t.dcpData.oldACs.toString),
    ColumnDef("ACsAdd", t => t.dcpData.oldACsAdd.toString),
    ColumnDef("Ldr", t => t.dcpData.oldLeaders.toString),
    ColumnDef("LdrsAdd", t => t.dcpData.oldLeadersAdd.toString),
    ColumnDef("Level1s", t => t.dcpData.p1Level1s.toString),
    ColumnDef("Level2s", t => t.dcpData.p2Level2s.toString),
    ColumnDef("Level2sAdd", t => t.dcpData.p3Level2sAdd.toString),
    ColumnDef("Level3s", t => t.dcpData.p4Level3s.toString),
    ColumnDef("Level45DTMs", t => t.dcpData.p5Level45D.toString),
    ColumnDef("Level45DTMsAdd", t => t.dcpData.p6Level45DAdd.toString),
    ColumnDef("NewMembers", t => t.dcpData.newMembers.toString),
    ColumnDef("AddNewMembers", t => t.dcpData.addNewMembers.toString),
    ColumnDef("OfficersTrainedRd1", t => t.dcpData.officersTrainedRd1.toString),
    ColumnDef("OfficersTrainedRd2", t => t.dcpData.officersTrainedRd2.toString),
    ColumnDef("COTMet", t => t.cotMet.toString),
    ColumnDef("MembersDuesOnTimeOct", t => t.memDuesOnTimeOct.toString),
    ColumnDef("MembersDuesOnTimeApr", t => t.memDuesOnTimeApr.toString),
    ColumnDef("OfficerListOnTime", t => t.dcpData.officerListOnTime.toString),
    // computed
    ColumnDef("Goal10Met", t => t.goal10Met.toString),
    ColumnDef("DistinguishedStatus", t => t.clubDistinctiveStatus),
    ColumnDef("MembersGrowth", t => t.membershipGrowth.toString),
    ColumnDef("AwardsPerMember", t => df2dp.format(t.awardsPerMember)),
    ColumnDef("DCPEligibility", t => t.dcpEligibility.toString),
    ColumnDef("MonthlyGrowth", t => t.monthlyGrowth.toString),
    ColumnDef("30SeptMembers", t => t.members30Sept.toString),
    ColumnDef("31MarMembers", t => t.members31Mar.toString),
    ColumnDef("Region", _ => "XXX"),
    ColumnDef("NovADVisit", t => t.divData.map(_.novADVisit.toString).getOrElse("")),
    ColumnDef("MayADVisit", t => t.divData.map(_.mayADVisit.toString).getOrElse("")),
    // from district report
    ColumnDef("TotalNewMembers", t => t.distData.map(_.totalNewMembers.toString).getOrElse("")),
    ColumnDef("LateRenewals", t => t.distData.map(_.lateRenewals.toString).getOrElse("")),
    ColumnDef("OctRenewals", t => t.distData.map(_.octRenewals.toString).getOrElse("")),
    ColumnDef("AprRenewals", t => t.distData.map(_.aprRenewals.toString).getOrElse("")),
    ColumnDef("TotalCharter", t => t.distData.map(_.totalCharter.toString).getOrElse("")),
    ColumnDef("TotalToDate", t => t.distData.map(_.totalToDate.toString).getOrElse("")),
    ColumnDef("CharterSuspendDate", t => t.distData.map(_.charterSuspendDate).getOrElse(""))
  )

}
