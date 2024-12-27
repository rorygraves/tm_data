package com.github.rorygraves.tm_data.data.club.perf

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df2dp
import slick.collection.heterogeneous._
import slick.jdbc.PostgresProfile.api._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length

import java.time.LocalDate

class HistoricClubPerfTable(val dbRunner: DBRunner) extends AbstractTable[TMClubDataPoint] {

  val tableName = "club_perf_historical"

  private val districtColumnId     = "district"
  private val monthColumnId        = "program_month"
  private val asOfDateColumnId     = "as_of_date"
  private val programYearColumnId  = "program_year"
  private val clubNumberColumnId   = "club_number"
  private val monthEndDateColumnId = "month_end_date"

  class HistoricalClubPerfTable(tag: Tag) extends Table[TMClubDataPoint](tag, tableName) {
    val programYear          = column[Int](programYearColumnId)
    val month                = column[Int](monthColumnId)
    val monthEndDate         = column[LocalDate](monthEndDateColumnId)
    val asOfDate             = column[LocalDate](asOfDateColumnId)
    val district             = column[String](districtColumnId, Length(3))
    val division             = column[String]("division", Length(2))
    val area                 = column[String]("area", Length(2))
    val clubNumber           = column[Int](clubNumberColumnId)
    val clubName             = column[String]("club_name")
    val clubStatus           = column[String]("club_status", Length(10))
    val memBase              = column[Int]("base_members")
    val activeMembers        = column[Int]("active_members")
    val goalsMet             = column[Int]("goals_met")
    val ccs                  = column[Int]("ccs")
    val ccsAdd               = column[Int]("ccs_add")
    val acs                  = column[Int]("acs")
    val acsAdd               = column[Int]("acs_add")
    val ldr                  = column[Int]("ldr")
    val ldrsAdd              = column[Int]("ldrs_add")
    val level1s              = column[Int]("level1s")
    val level2s              = column[Int]("level2s")
    val level2sAdd           = column[Int]("level2s_add")
    val level3s              = column[Int]("level3s")
    val level45dtms          = column[Int]("level45dtms")
    val level45dtmsAdd       = column[Int]("level45dtms_add")
    val newMembers           = column[Int]("new_members")
    val addNewMembers        = column[Int]("add_new_members")
    val officersTrainedRd1   = column[Int]("officers_trained_rd1")
    val officersTrainedRd2   = column[Int]("officers_trained_rd2")
    val cotMet               = column[Boolean]("cot_met")
    val membersDuesOnTimeOct = column[Boolean]("members_dues_on_time_oct")
    val membersDuesOnTimeApr = column[Boolean]("members_dues_on_time_apr")
    val officerListOnTime    = column[Boolean]("officer_list_on_time")
    val goal10Met            = column[Boolean]("goal_10_met")
    val distinguishedStatus  = column[Option[String]]("distinguished_status", Length(2))
    val membersGrowth        = column[Int]("members_growth")
    val awardsPerMember      = column[Double]("awards_per_member")
    val dcpEligibility       = column[Boolean]("dcp_eligibility")
    val monthlyGrowth        = column[Int]("monthly_growth")
    val members30Sept        = column[Int]("members_30_sept")
    val members31Mar         = column[Int]("members_31_mar")
    val region               = column[String]("region", Length(4))
    val novAdVisit           = column[Boolean]("nov_ad_visit")
    val mayAdVisit           = column[Boolean]("may_ad_visit")
    val totalNewMembers      = column[Int]("total_new_members")
    val lateRenewals         = column[Int]("late_renewals")
    val octRenewals          = column[Int]("oct_renewals")
    val aprRenewals          = column[Int]("apr_renewals")
    val totalCharter         = column[Int]("total_charter")
    val totalToDate          = column[Int]("total_to_date")
    val charterSuspendDate   = column[Option[String]]("charter_suspend_date", Length(50))

    val idx = index(s"idx_${this.tableName}_club_year_month", (programYear, month, clubNumber), unique = true)

    val idx2 = index(s"idx_${this.tableName}_year_district_division", (programYear, district, division), unique = false)

    val pk = primaryKey("pk_" + this.tableName, (clubNumber, monthEndDate))

    def clubDCPDataProjection = (
      ccs ::
        ccsAdd ::
        acs ::
        acsAdd ::
        ldr ::
        ldrsAdd ::
        level1s ::
        level2s ::
        level2sAdd ::
        level3s ::
        level45dtms ::
        level45dtmsAdd ::
        officerListOnTime ::
        goal10Met ::
        officersTrainedRd1 ::
        officersTrainedRd2 ::
        cotMet ::
        membersDuesOnTimeApr ::
        membersDuesOnTimeOct ::
        newMembers ::
        addNewMembers :: HNil
    ).mapTo[ClubDCPData]

    // projection for TMDivClubDataPoint
    def divDataProjection = (
      novAdVisit,
      mayAdVisit
    ) <> ((TMClubDivData.apply _).tupled, TMClubDivData.unapply)

    // projection for TMDistClubDataPoint
    def distDataProjection = (
      totalNewMembers,
      lateRenewals,
      octRenewals,
      aprRenewals,
      totalCharter,
      totalToDate,
      charterSuspendDate
    ) <> ((TMClubDistData.apply _).tupled, TMClubDistData.unapply)

    // projection for TMClubDataPoint
    def * = (
      programYear ::
        month ::
        monthEndDate ::
        asOfDate ::
        district ::
        region ::
        division ::
        area ::
        clubNumber ::
        clubName ::
        clubStatus ::
        memBase ::
        activeMembers ::
        membersGrowth ::
        awardsPerMember ::
        dcpEligibility ::
        goalsMet ::
        clubDCPDataProjection ::
        distinguishedStatus ::
        monthlyGrowth ::
        members30Sept ::
        members31Mar ::
        divDataProjection ::
        distDataProjection :: HNil
    ).mapTo[TMClubDataPoint]

  }

  def latestClubsForArea(district: String, division: String, area: String): List[TMClubDataPoint] = {
    val base                  = tq.filter(r => r.district === district && r.division === division && r.area === area)
    val latestMonthEndForArea = base.map(_.monthEndDate).max

    val query = base.filter(_.monthEndDate === latestMonthEndForArea).result
    dbRunner.dbAwait(query).toList
  }

  val tq = TableQuery[HistoricalClubPerfTable]

  def clubDataByYear(clubId: Int, curYear: Int): List[TMClubDataPoint] = {
    dbRunner
      .dbAwait(tq.filter(r => r.clubNumber === clubId && r.programYear === curYear).result)
      .sortBy(_.monthEndDate)
      .toList
  }

  private val clubNumberColumn     = IntColumn[TMClubDataPoint](clubNumberColumnId, t => t.clubNumber)
  private val clubNameColumn       = StringColumn[TMClubDataPoint]("club_name", t => t.clubName)
  private val clubStatusColumn     = StringColumn[TMClubDataPoint]("club_status", t => t.clubStatus)
  private val baseMembersColumn    = IntColumn[TMClubDataPoint]("base_members", t => t.membershipBase)
  private val activeMembersColumn  = IntColumn[TMClubDataPoint]("active_members", t => t.activeMembers)
  private val goalsMetColumn       = IntColumn[TMClubDataPoint]("goals_met", t => t.goalsMet)
  private val ccsColumn            = IntColumn[TMClubDataPoint]("ccs", t => t.dcpData.oldCCs)
  private val ccsAddColumn         = IntColumn[TMClubDataPoint]("ccs_add", t => t.dcpData.oldCCsAdd)
  private val acsColumn            = IntColumn[TMClubDataPoint]("acs", t => t.dcpData.oldACs)
  private val acsAddColumn         = IntColumn[TMClubDataPoint]("acs_add", t => t.dcpData.oldACsAdd)
  private val ldrColumn            = IntColumn[TMClubDataPoint]("ldr", t => t.dcpData.oldLeaders)
  private val ldrsAddColumn        = IntColumn[TMClubDataPoint]("ldrs_add", t => t.dcpData.oldLeadersAdd)
  private val level1sColumn        = IntColumn[TMClubDataPoint]("level1s", t => t.dcpData.p1Level1s)
  private val level2sColumn        = IntColumn[TMClubDataPoint]("level2s", t => t.dcpData.p2Level2s)
  private val level2sAddColumn     = IntColumn[TMClubDataPoint]("level2s_add", t => t.dcpData.p3Level2sAdd)
  private val level3sColumn        = IntColumn[TMClubDataPoint]("level3s", t => t.dcpData.p4Level3s)
  private val level45dtmsColumn    = IntColumn[TMClubDataPoint]("level45dtms", t => t.dcpData.p5Level45D)
  private val level45dtmsAddColumn = IntColumn[TMClubDataPoint]("level45dtms_add", t => t.dcpData.p6Level45DAdd)
  private val newMembersColumn     = IntColumn[TMClubDataPoint]("new_members", t => t.dcpData.newMembers)
  private val addNewMembersColumn  = IntColumn[TMClubDataPoint]("add_new_members", t => t.dcpData.addNewMembers)
  private val officersTrainedRd1Column =
    IntColumn[TMClubDataPoint]("officers_trained_rd1", t => t.dcpData.officersTrainedRd1)
  private val officersTrainedRd2Column =
    IntColumn[TMClubDataPoint]("officers_trained_rd2", t => t.dcpData.officersTrainedRd2)
  private val cotMetColumn = BooleanColumn[TMClubDataPoint]("cot_met", t => t.dcpData.cotMet)
  private val membersDuesOnTimeOctColumn =
    BooleanColumn[TMClubDataPoint]("members_dues_on_time_oct", t => t.dcpData.memDuesOnTimeOct)
  private val membersDuesOnTimeAprColumn =
    BooleanColumn[TMClubDataPoint]("members_dues_on_time_apr", t => t.dcpData.memDuesOnTimeApr)
  private val officerListOnTimeColumn =
    BooleanColumn[TMClubDataPoint]("officer_list_on_time", t => t.dcpData.officerListOnTime)
  private val goal10MetColumn = BooleanColumn[TMClubDataPoint]("goal_10_met", t => t.dcpData.goal10Met)
  private val distinguishedStatusColumn =
    OptionalStringColumn[TMClubDataPoint]("distinguished_status", t => t.clubDistinguishedStatus)
  private val membersGrowthColumn = IntColumn[TMClubDataPoint]("members_growth", t => t.membershipGrowth)
  private val awardsPerMemberColumn =
    DoubleColumn[TMClubDataPoint]("awards_per_member", t => t.awardsPerMember, df2dp)
  private val dcpEligibilityColumn = BooleanColumn[TMClubDataPoint]("dcp_eligibility", t => t.dcpEligibility)
  private val monthlyGrowthColumn  = IntColumn[TMClubDataPoint]("monthly_growth", t => t.monthlyGrowth)
  private val members30SeptColumn  = IntColumn[TMClubDataPoint]("members_30_sept", t => t.members30Sept)
  private val members31MarColumn   = IntColumn[TMClubDataPoint]("members_31_mar", t => t.members31Mar)
  private val regionColumn         = StringColumn[TMClubDataPoint]("region", t => t.region)
  private val novAdVisitColumn     = BooleanColumn[TMClubDataPoint]("nov_ad_visit", t => t.divData.novADVisit)

  private val mayAdVisitColumn = BooleanColumn[TMClubDataPoint]("may_ad_visit", t => t.divData.mayADVisit)
  private val totalNewMembersColumn =
    IntColumn[TMClubDataPoint]("total_new_members", t => t.distData.totalNewMembers)
  private val lateRenewalsColumn =
    IntColumn[TMClubDataPoint]("late_renewals", t => t.distData.lateRenewals)
  private val octRenewalsColumn =
    IntColumn[TMClubDataPoint]("oct_renewals", t => t.distData.octRenewals)
  private val aprRenewalsColumn =
    IntColumn[TMClubDataPoint]("apr_renewals", t => t.distData.aprRenewals)
  private val totalCharterColumn =
    IntColumn[TMClubDataPoint]("total_charter", t => t.distData.totalCharter)
  private val totalToDateColumn =
    IntColumn[TMClubDataPoint]("total_to_date", t => t.distData.totalToDate)

  def clubData(
      district: String,
      year: Option[Int],
      month: Option[Int],
      division: Option[String],
      area: Option[String],
      club: Option[Int]
  ) = {
    val query = tq
      .filter(_.district === district)
      .filterOpt(year)((t, y) => t.programYear === y)
      .filterOpt(month)((t, m) => t.month === m)
      .filterOpt(division)((t, d) => t.division === d)
      .filterOpt(area)((t, a) => t.area === a)
      .filterOpt(club)((t, c) => t.clubNumber === c)
    dbRunner.dbAwait(query.result, "HistoricalClubPerfTableDef.clubData").toList
  }

  val columns: List[Column[TMClubDataPoint]] = List(
    IntColumn(programYearColumnId, t => t.programYear),
    IntColumn(monthColumnId, t => t.month),
    LocalDateColumn(monthEndDateColumnId, t => t.monthEndDate),
    LocalDateColumn(asOfDateColumnId, t => t.asOfDate),
    StringColumn("district", t => t.district),
    StringColumn("division", t => t.division),
    StringColumn("area", t => t.area),
    clubNumberColumn,
    clubNameColumn,
    clubStatusColumn,
    baseMembersColumn,
    activeMembersColumn,
    goalsMetColumn,
    ccsColumn,
    ccsAddColumn,
    acsColumn,
    acsAddColumn,
    ldrColumn,
    ldrsAddColumn,
    level1sColumn,
    level2sColumn,
    level2sAddColumn,
    level3sColumn,
    level45dtmsColumn,
    level45dtmsAddColumn,
    newMembersColumn,
    addNewMembersColumn,
    officersTrainedRd1Column,
    officersTrainedRd2Column,
    cotMetColumn,
    membersDuesOnTimeOctColumn,
    membersDuesOnTimeAprColumn,
    officerListOnTimeColumn,
    goal10MetColumn,
    distinguishedStatusColumn,
    membersGrowthColumn,
    awardsPerMemberColumn,
    dcpEligibilityColumn,
    monthlyGrowthColumn,
    members30SeptColumn,
    members31MarColumn,
    regionColumn,
    novAdVisitColumn,
    mayAdVisitColumn,
    totalNewMembersColumn,
    lateRenewalsColumn,
    octRenewalsColumn,
    aprRenewalsColumn,
    totalCharterColumn,
    totalToDateColumn,
    OptionalStringColumn[TMClubDataPoint]("charter_suspend_date", t => t.distData.charterSuspendDate)
  )

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    println("HistoricAreaPerfTableDef.createIfNotExists-------------------------------------------")
    createTableFromStatements(statements)
  }

  def latestDistrictMonthDates(): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths()
    allDistYearMonths
      .groupBy(_._1)
      .view
      .mapValues { distData =>
        val sorted = distData.sortBy(_._4)
        val last   = sorted.last
        (last._2, last._3)
      }
      .toMap

  }

  def existsByYearMonthDistrict(progYear: Int, month: Int, districtId: String): Boolean = {
    searchByDistrict(districtId, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def allDistrictYearMonths(): Seq[(String, Int, Int, LocalDate)] = {
    val query = tq.map(t => (t.district, t.programYear, t.month, t.monthEndDate)).distinct.result
    dbRunner.dbAwait(query).toList
  }

  def searchByDistrict(
      district: String,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[TMClubDataPoint] = {
    val baseQuery = tq
      .filter(_.district === district)
      .filterOpt(progYear)((t, y) => t.programYear === y)
      .filterOpt(month)((t, m) => t.month === m)

    val query = limit match {
      case Some(l) => baseQuery.take(l)
      case None    => baseQuery
    }

    dbRunner.dbAwait(query.result, "HistoricalClubPerfTableDef.searchByDistrict").toList
  }

  def findByClubYearMonth(
      clubNumber: Int,
      programYear: Int,
      month: Int
  ): Option[TMClubDataPoint] = {
    val query =
      tq.filter(t => t.clubNumber === clubNumber && t.programYear === programYear && t.month === month).take(1).result
    dbRunner.dbAwait(query).headOption
  }

  def insertOrUpdate(monthData: List[TMClubDataPoint]): Int = {

    val statements = monthData.map(tq.insertOrUpdate(_))
    val seq        = DBIO.sequence(statements).transactionally
    val res        = dbRunner.dbAwait(seq, "HistoricClubPerfTableDef.insertOrUpdate")
    res.sum
  }

  def latestClubsForDistrict(tmId: String): List[TMClubDataPoint] = {
    val latestMonthEndForDistrict = tq.filter(_.district === tmId).map(_.monthEndDate).max

    val query = tq.filter(r => r.district === tmId && r.monthEndDate === latestMonthEndForDistrict).result
    dbRunner.dbAwait(query).toList
  }

  def latestForClub(clubId: Int): Option[TMClubDataPoint] = {
    val latestMonthEndForClub = tq.filter(_.clubNumber === clubId).map(_.monthEndDate).max

    val query = tq.filter(r => r.clubNumber === clubId && r.monthEndDate === latestMonthEndForClub).take(1).result
    dbRunner.dbAwait(query).headOption
  }

  def activeMembersByDate(district: String, programYear: Int): List[((Int, LocalDate), Int)] = {
    val query =
      tq
        .filter(r => r.district === district && r.programYear === programYear)
        .groupBy(r => (r.programYear, r.asOfDate))
        .map { case (date, group) => (date, group.map(_.activeMembers).sum) }
        .result

    // program year, as of date -> active members
    val res: List[((Int, LocalDate), Option[Int])] = dbRunner.dbAwait(query).toList
    res
      .flatMap {
        case (_, None)           => None
        case (date, Some(count)) => Some((date, count))
      }
      .sortBy(_._1._2)
  }

  // returns (month, as of date) -> active members
  def activeMembersByMonth(district: String, programYear: Int): List[((Int, LocalDate), Int)] = {
    val query =
      tq
        .filter(r => r.district === district && r.programYear === programYear)
        .groupBy(r => (r.month, r.asOfDate))
        .map { case (key, group) => (key, group.map(_.activeMembers).sum) }
        .result

    // program year, as of date -> active members
    dbRunner.dbAwait(query).toList.map(x => (x._1, x._2.getOrElse(0))).sortBy(_._1._2)

  }

}
