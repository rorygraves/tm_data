package app.club

import app.Main.{ColumnCalculator, decimalFormatter}

object HistoricClubData {

  case class ColumnCalculation(
      name: String,
      calculation: TMClubDataPoint => String
  ) extends ColumnCalculator

  val clubColumnGenerator: List[ColumnCalculator] = List[ColumnCalculator](
    ColumnCalculation("District", t => t.district),
    ColumnCalculation("Division", t => t.division),
    ColumnCalculation("Area", t => t.area),
    ColumnCalculation("ProgramYear", t => t.programYear.toString),
    ColumnCalculation("Month", t => t.month.toString),
    ColumnCalculation("AsOfDate", t => t.asOfDate.toString),
    ColumnCalculation("ClubNumber", t => t.clubNumber),
    ColumnCalculation("ClubName", t => t.clubName),
    ColumnCalculation("ClubStatus", t => t.clubStatus),
    ColumnCalculation("BaseMembers", t => t.memBase.toString),
    ColumnCalculation("ActiveMembers", t => t.activeMembers.toString),
    ColumnCalculation("GoalsMet", t => t.goalsMet.toString),
    ColumnCalculation("CCs", t => t.dcpData.oldCCs.toString),
    ColumnCalculation("CCsAdd", t => t.dcpData.oldCCsAdd.toString),
    ColumnCalculation("ACs", t => t.dcpData.oldACs.toString),
    ColumnCalculation("ACsAdd", t => t.dcpData.oldACsAdd.toString),
    ColumnCalculation("Ldr", t => t.dcpData.oldLeaders.toString),
    ColumnCalculation("LdrsAdd", t => t.dcpData.oldLeadersAdd.toString),
    ColumnCalculation("Level1s", t => t.dcpData.level1s.toString),
    ColumnCalculation("Level2s", t => t.dcpData.level2s.toString),
    ColumnCalculation("Level2sAdd", t => t.dcpData.level2sAdd.toString),
    ColumnCalculation("Level3s", t => t.dcpData.level3s.toString),
    ColumnCalculation("Level45DTMs", t => t.dcpData.level4s.toString),
    ColumnCalculation("Level45DTMsAdd", t => t.dcpData.level5s.toString),
    ColumnCalculation("NewMembers", t => t.dcpData.newMembers.toString),
    ColumnCalculation("AddNewMembers", t => t.dcpData.addNewMembers.toString),
    ColumnCalculation(
      "OfficersTrainedRd1",
      t => t.dcpData.officersTrainedRd1.toString
    ),
    ColumnCalculation(
      "OfficersTrainedRd2",
      t => t.dcpData.officersTrainedRd2.toString
    ),
    ColumnCalculation("MembersDuesOnTimeOct", t => t.memDuesOnTimeOct.toString),
    ColumnCalculation("MembersDuesOnTimeApr", t => t.memDuesOnTimeApr.toString),
    ColumnCalculation(
      "OfficerListOnTime",
      t => t.dcpData.officerListOnTime.toString
    ),
    ColumnCalculation("COTMet", t => t.cotMet.toString),
    ColumnCalculation("Goal10Met", t => t.goal10Met.toString),
    ColumnCalculation("DistinguishedStatus", t => t.clubDistinctiveStatus),
    ColumnCalculation("MembersGrowth", t => t.membershipGrowth.toString),
    ColumnCalculation(
      "AwardsPerMember",
      t => decimalFormatter.format(t.awardsPerMember)
    ),
    ColumnCalculation("DCPEligibility", t => t.dcpEligibility.toString),
    ColumnCalculation("MonthlyGrowth", t => t.monthlyGrowth.toString),
    ColumnCalculation("30SeptMembers", t => t.members30Sept.toString),
    ColumnCalculation("31MarMembers", t => t.members31Mar.toString),
    ColumnCalculation("Region", _ => "XXX"),
    ColumnCalculation("NovADVisit", t => t.novADVisit.toString),
    ColumnCalculation("MarADVisit", t => t.mayADVisit.toString)
  )

}
