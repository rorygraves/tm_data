package com.github.rorygraves.tm_data.data.club.perf

import java.time.LocalDate

object TMClubDataPoint {}

case class TMClubDataPoint(
    programYear: Int,
    programMonth: Int,
    monthEndDate: LocalDate,
    asOfDate: LocalDate,
    district: String,
    region: String,
    division: String,
    area: String,
    clubNumber: Int,
    clubName: String,
    clubStatus: String,
    membershipBase: Int,
    activeMembers: Int,
    membershipGrowth: Int,
    awardsPerMember: Double,
    dcpEligibility: Boolean,
    goalsMet: Int,
    dcpData: ClubDCPData,
    clubDistinguishedStatus: Option[String],
    monthlyGrowth: Int,
    members30Sept: Int,
    members31Mar: Int,
    divData: TMClubDivData,
    distData: TMClubDistData,
    csp: String = "N"
) extends Ordered[TMClubDataPoint] {

  override def compare(that: TMClubDataPoint): Int = {

    // compare by year, by month (order 7-12,1-6), asOfDate, clubNumber
    val yearCompare = programYear.compareTo(that.programYear)
    if (yearCompare != 0) {
      return yearCompare
    }
    val monthCompare       = if (programMonth >= 7) programMonth else programMonth + 12
    val thatMonthCompare   = if (that.programMonth >= 7) that.programMonth else that.programMonth + 12
    val monthCompareResult = monthCompare.compareTo(thatMonthCompare)
    if (monthCompareResult != 0) {
      return monthCompareResult
    }
    val asOfDateCompare = asOfDate.compareTo(that.asOfDate)
    if (asOfDateCompare != 0) {
      return asOfDateCompare
    }
    clubNumber.compareTo(that.clubNumber)

  }

  def eligibleDCPPoints: Int = if (dcpEligibility) goalsMet else 0

  def isActive: Boolean    = clubStatus == "Active"
  def isSuspended: Boolean = clubStatus == "Suspended"
  def isLow: Boolean       = clubStatus == "Low"

  def isIneligible: Boolean = clubStatus == "Ineligible"
  def isInactive: Boolean   = isIneligible || isLow || isSuspended
}
