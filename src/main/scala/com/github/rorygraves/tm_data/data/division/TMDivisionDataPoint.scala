package com.github.rorygraves.tm_data.data.division

import java.time.LocalDate

case class TMDivisionDataPoint(
    programYear: Int,
    month: Int,
    monthEndDate: LocalDate,
    asOfDate: LocalDate,
    district: String,
    division: String,
    clubBase: Int,
    paidClubsReqForDist: Int,
    paidClubsReqForSelect: Int,
    paidClubsReqForPres: Int,
    curPaidClubs: Int,
    distClubsReqDist: Int,
    distClubsReqSelect: Int,
    distClubsReqPres: Int,
    curDistClubs: Int,
    distinguishedStatus: String
) {}
