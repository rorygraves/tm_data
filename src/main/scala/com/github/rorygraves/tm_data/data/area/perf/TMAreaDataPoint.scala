package com.github.rorygraves.tm_data.data.area.perf

import java.time.LocalDate

case class TMAreaDataPoint(
    programYear: Int,
    month: Int,
    monthEndDate: LocalDate,
    asOfDate: LocalDate,
    district: String,
    division: String,
    area: String,
    paidClubBase: Int,
    paidClubsReqForDist: Int,
    clubsReqForSelect: Int,
    clubsReqForPres: Int,
    curPaidClubs: Int,
    distClubsReqDist: Int,
    distClubsReqSelect: Int,
    distClubsReqPres: Int,
    curDistClubs: Int,
    distinguishedStatus: String
) {}
