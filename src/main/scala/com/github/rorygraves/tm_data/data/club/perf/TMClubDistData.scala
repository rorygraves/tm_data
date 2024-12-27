package com.github.rorygraves.tm_data.data.club.perf

/** Club level data from the district report */
case class TMClubDistData(
    totalNewMembers: Int,
    lateRenewals: Int,
    octRenewals: Int,
    aprRenewals: Int,
    totalCharter: Int,
    totalToDate: Int,
    charterSuspendDate: Option[String]
)
