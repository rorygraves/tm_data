package com.github.rorygraves.tm_data.data.club.perf.historical.data

case class TMClubDivData(
    octRenewals: Int,
    aprRenewals: Int,
    novADVisit: Boolean,
    mayADVisit: Boolean
    // duplicated in club level data
//    activeMembers: Int,
//    goalsMet: Int,
//    distinguishedStatus: String
)
