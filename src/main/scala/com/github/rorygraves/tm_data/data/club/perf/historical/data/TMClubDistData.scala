package com.github.rorygraves.tm_data.data.club.perf.historical.data

/** Club level data from the district report */
case class TMClubDistData(
    totalNewMembers: Int,
    lateRenewals: Int,
    totalCharter: Int,
    totalToDate: Int,
    charterSuspendDate: String
)
