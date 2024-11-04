package app.club.perf.historical.data

import java.time.LocalDate

case class ClubDCPData(
    programYear: Int,
    month: Int,
    asOfDate: LocalDate,
    clubId: String,
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
    p5Level45D_orig: Int,
    p6Level45DAdd_orig: Int,
    officerListOnTime: Boolean,
    officersTrainedRd1: Int,
    officersTrainedRd2: Int,
    newMembers: Int,
    addNewMembers: Int
) {

  // compute p5 based on ERD spec due to changing meaning in TI
  val p5Level45D: Int = {

    val level4s = p5Level45D_orig
    val level5s = p6Level45DAdd_orig

    if (programYear >= 2016 && programYear <= 2019) {
      if (level4s == 1 || level5s == 1) 1
      else if (level4s == 1 && level5s == 1) 1
      else if (level4s > 1 || level5s > 1) 1
      else if (level4s > 1 && level5s > 1) 1
      else
        0
    } else
      p5Level45D_orig

  }

  // compute p5 based on ERD spec due to changing meaning in TI
  val p6Level45DAdd: Int = {
    val level4s = p5Level45D_orig
    val level5s = p6Level45DAdd_orig

    if (programYear >= 2016 && programYear <= 2019) {
      if (level4s == 1 || level5s == 1) 0
      else if (level4s >= 1 && level5s >= 1) level4s + level5s - 1
      else if (level4s > 1 || level5s > 1) level4s + level5s - 1
      else 0
    } else {
      p6Level45DAdd_orig
    }
  }

  val totalAwards: Int =
    oldCCs + oldCCsAdd + oldACs + oldACsAdd + oldLeaders + oldLeadersAdd +
      p1Level1s + p2Level2s + p3Level2sAdd + p4Level3s + p5Level45D + p6Level45DAdd

}

object ClubDCPData {
  def fromDistrictClubReportCSV(
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: String,
      data: Map[String, String]
  ): ClubDCPData = {

    // p5/p6 change column name
    val p5Level45D_orig = data.getOrElse("Level 4s, Level 5s, or DTM award", data.getOrElse("Level 4s", "0")).toInt
    val p6Level45DAdd_orig =
      data.getOrElse("Add. Level 4s, Level 5s, or DTM award", data.getOrElse("Level 5s", "0")).toInt

    ClubDCPData(
      programYear,
      month,
      asOfDate,
      clubNumber,
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
      p5Level45D_orig,
      p6Level45DAdd_orig,
      data("Off. List On Time").toInt > 0,
      data("Off. Trained Round 1").toInt,
      data("Off. Trained Round 2").toInt,
      data("New Members").toInt,
      data("Add. New Members").toInt
    )
  }
}
