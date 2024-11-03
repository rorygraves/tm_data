package app.club.perf.historical.data

case class ClubDCPData(
    programYear: Int,
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
  val totalAwards: Int =
    oldCCs + oldCCsAdd + oldACs + oldACsAdd + oldLeaders + oldLeadersAdd + p1Level1s + p2Level2s + p3Level2sAdd + p4Level3s + p5Level45D_orig + p6Level45DAdd

  // P5
  val p5Level45D: Int = {

    val level4s = p5Level45D_orig
    val level5s = p6Level45DAdd_orig

    // IF ProgramYear = 2016 OR ProgramYear = 2017 OR ProgramYear = 2018 OR ProgramYear = 2019
    //	THEN IF Level4s = 1 OR Level5s = 1
    //		THEN 1
    //	ELSE IF Levels4 = 1 AND Level5s = 1
    //		THEN 1
    //	ELSE IF Levels4 < 1 OR/AND Level5s < 1
    //		THEN 1
    //	ELSE 0
    if (programYear >= 2016 && programYear <= 2019) {
      if (level4s == 1 || level5s == 1) {
        1
      } else if (level4s == 1 && level5s == 1) {
        1
      } else if (level4s > 1 || level5s > 1) {
        1
      } else if (level4s > 1 && level5s > 1) {
        1
      } else
        0

    } else
      p5Level45D_orig

  }
  // P6
  val p6Level45DAdd: Int = {
    p6Level45DAdd_orig
  }
//    val level4s = p5Level45D_orig
//    val level5s = p6Level45DAdd_orig
//
////    IF ProgramYear = 2016 OR ProgramYear = 2017 OR ProgramYear = 2018 OR ProgramYear = 2019
////    THEN IF Level4s = 1 OR Level5s = 1
////    THEN 0
////    ELSE IF Levels4 = 1 AND Level5s = 1
////    THEN 1
////    ELSE IF Levels4 < 1 OR/AND Level5s < 1
////    THEN (Levels4 - 1) + (Levels5 - 1) #The result of either operations cannot be negative!
////      ELSE 0
//
//    if(programYear >= 2016 && programYear <=2019) {
//      level4s + level5s
//    } else {
//      level4s
//    }
//  }
}

object ClubDCPData {
  def fromDistrictClubReportCSV(programYear: Int, data: Map[String, String]): ClubDCPData = {
    ClubDCPData(
      programYear,
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
