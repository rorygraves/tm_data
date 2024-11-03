package app.club.perf.historical.data

case class ClubDCPData(
    oldCCs: Int,
    oldCCsAdd: Int,
    oldACs: Int,
    oldACsAdd: Int,
    oldLeaders: Int,
    oldLeadersAdd: Int,
    p1Level1s: Int,
    p2Level2s: Int,
    p3Level2sAdd: Int,
    p4Level3s: Int,
    p5Level45D: Int,
    p6Level45DAdd: Int,
    officerListOnTime: Boolean,
    officersTrainedRd1: Int,
    officersTrainedRd2: Int,
    newMembers: Int,
    addNewMembers: Int
) {
  val totalAwards: Int =
    oldCCs + oldCCsAdd + oldACs + oldACsAdd + oldLeaders + oldLeadersAdd + p1Level1s + p2Level2s + p3Level2sAdd + p4Level3s + p5Level45D + p6Level45DAdd
}

object ClubDCPData {
  def fromDistrictClubReportCSV(
      data: Map[String, String]
  ): ClubDCPData = {
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
      data.getOrElse("Level 4s, Level 5s, or DTM award", "0").toInt,
      data.getOrElse("Add. Level 4s, Level 5s, or DTM award", "0").toInt,
      data("Off. List On Time").toInt > 0,
      data("Off. Trained Round 1").toInt,
      data("Off. Trained Round 2").toInt,
      data("New Members").toInt,
      data("Add. New Members").toInt
    )
  }
}
