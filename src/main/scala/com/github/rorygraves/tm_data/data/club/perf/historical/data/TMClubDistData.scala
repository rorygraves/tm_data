package com.github.rorygraves.tm_data.data.club.perf.historical.data

case class TMClubDistData(
    totalNewMembers: Int,
    lateRenewals: Int,
    // duplicated in div level data
//    octRenewals: Int,
//    aprRenewals: Int,
    totalCharter: Int,
    totalToDate: Int,
    charterSuspendDate: String
)
