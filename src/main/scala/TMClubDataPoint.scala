import java.time.LocalDate

object TMClubDataPoint {
  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      data: Map[String, String]
  ): TMClubDataPoint = {
    TMClubDataPoint(
      programYear,
      month,
      asOfDate,
      data("District"),
      data("Division"),
      data("Area"),
      data("Club Number"),
      data("Club Name"),
      data("Club Status"),
      data("Mem. Base").toInt,
      data("Active Members").toInt,
      data("Goals Met").toInt,
      data("Mem. dues on time Apr") == "1",
      data("Mem. dues on time Oct") == "1",
      data("Club Distinguished Status"),
      ClubDCPData.fromDistrictClubReportCSV(data)
    )
  }
}

case class TMClubDataPoint(
    programYear: Int,
    month: Int,
    asOfDate: LocalDate,
    district: String,
    division: String,
    area: String,
    clubNumber: String,
    clubName: String,
    clubStatus: String,
    memBase: Int,
    activeMembers: Int,
    goalsMet: Int,
    memDuesOnTimeApr: Boolean,
    memDuesOnTimeOct: Boolean,
    clubDistinctiveStatus: String,
    dcpData: ClubDCPData
) {
  lazy val cotMet: Boolean =
    dcpData.officersTrainedRd1 > 3 && dcpData.officersTrainedRd2 > 3

  lazy val goal10Met: Boolean =
    (memDuesOnTimeApr || memDuesOnTimeOct) && dcpData.officerListOnTime

  lazy val membershipGrowth: Int = Math.max(activeMembers - memBase, 0)

  lazy val awardsPerMember: Double =
    if (activeMembers > 0) dcpData.totalAwards.toDouble / activeMembers else 0.0

  lazy val dcpEligability: Boolean =
    activeMembers > 19 || membershipGrowth > 2
}

object ClubDCPData {
  def fromDistrictClubReportCSV(data: Map[String, String]): ClubDCPData = {
    ClubDCPData(
      data.getOrElse("CCs", "0").toInt,
      data.getOrElse("Add. CCs", "0").toInt,
      data.getOrElse("ACs", "0").toInt,
      data.getOrElse("Add. ACs", "0").toInt,
      data.getOrElse("CL/AL/DTMs", "0").toInt,
      data.getOrElse("Add. CL/AL/DTMs", "0").toInt,
      data.getOrElse("Level 1s", "0").toInt,
      data.getOrElse("Level 2s", "0").toInt,
      data.getOrElse("Add. Level 2s", "0").toInt,
      data.getOrElse("Level 3s", "0").toInt,
      data.getOrElse("Level 4s", "0").toInt,
      data.getOrElse("Level 5s", "0").toInt,
      data("Off. List On Time").toInt > 0,
      data("Off. Trained Round 1").toInt,
      data("Off. Trained Round 2").toInt,
      data("New Members").toInt,
      data("Add. New Members").toInt
    )
  }
}

case class ClubDCPData(
    oldCCs: Int,
    oldCCsAdd: Int,
    oldACs: Int,
    oldACsAdd: Int,
    oldLeaders: Int,
    oldLeadersAdd: Int,
    level1s: Int,
    level2s: Int,
    level2sAdd: Int,
    level3s: Int,
    level4s: Int,
    level5s: Int,
    officerListOnTime: Boolean,
    officersTrainedRd1: Int,
    officersTrainedRd2: Int,
    newMembers: Int,
    addNewMembers: Int
) {
  val totalAwards =
    oldCCs + oldCCsAdd + oldACs + oldACsAdd + oldLeaders + oldLeadersAdd + level1s + level2s + level2sAdd + level3s + level4s + level5s
}
