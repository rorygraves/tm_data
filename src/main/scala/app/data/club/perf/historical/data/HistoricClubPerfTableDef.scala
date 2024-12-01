package app.data.club.perf.historical.data

import app.util.FormatUtil.df2dp
import app.db
import app.db._

import java.sql.ResultSet
import java.time.LocalDate

object HistoricClubPerfTableDef extends TableDef[TMClubDataPoint] {

  val tableName = "club_perf_historical"

  private val districtColumnId     = "district"
  private val monthColumnId        = "program_month"
  private val asOfDateColumnId     = "as_of_date"
  private val programYearColumnId  = "program_year"
  private val clubNumberColumnId   = "club_number"
  private val monthEndDateColumnId = "month_end_date"

  private val programYearColumn = IntColumnDef[TMClubDataPoint](programYearColumnId, t => t.programYear)
  private val monthColumn       = IntColumnDef[TMClubDataPoint](monthColumnId, t => t.month)
  private val monthEndDateColumn =
    LocalDateColumnDef[TMClubDataPoint](monthEndDateColumnId, t => t.monthEndDate, primaryKey = true)
  private val asOfDateColumn   = LocalDateColumnDef[TMClubDataPoint](asOfDateColumnId, t => t.asOfDate)
  private val districtColumn   = StringColumnDef[TMClubDataPoint]("district", t => t.district, length = 3)
  private val divisionColumn   = StringColumnDef[TMClubDataPoint]("division", t => t.division, length = 2)
  private val areaColumn       = StringColumnDef[TMClubDataPoint]("area", t => t.area, length = 2)
  private val clubNumberColumn = IntColumnDef[TMClubDataPoint](clubNumberColumnId, t => t.clubNumber, primaryKey = true)
  private val clubNameColumn   = StringColumnDef[TMClubDataPoint]("club_name", t => t.clubName)
  private val clubStatusColumn = StringColumnDef[TMClubDataPoint]("club_status", t => t.clubStatus, length = 10)
  private val baseMembersColumn    = IntColumnDef[TMClubDataPoint]("base_members", t => t.memBase)
  private val activeMembersColumn  = IntColumnDef[TMClubDataPoint]("active_members", t => t.activeMembers)
  private val goalsMetColumn       = IntColumnDef[TMClubDataPoint]("goals_met", t => t.goalsMet)
  private val ccsColumn            = IntColumnDef[TMClubDataPoint]("ccs", t => t.dcpData.oldCCs)
  private val ccsAddColumn         = IntColumnDef[TMClubDataPoint]("ccs_add", t => t.dcpData.oldCCsAdd)
  private val acsColumn            = IntColumnDef[TMClubDataPoint]("acs", t => t.dcpData.oldACs)
  private val acsAddColumn         = IntColumnDef[TMClubDataPoint]("acs_add", t => t.dcpData.oldACsAdd)
  private val ldrColumn            = IntColumnDef[TMClubDataPoint]("ldr", t => t.dcpData.oldLeaders)
  private val ldrsAddColumn        = IntColumnDef[TMClubDataPoint]("ldrs_add", t => t.dcpData.oldLeadersAdd)
  private val level1sColumn        = IntColumnDef[TMClubDataPoint]("level1s", t => t.dcpData.p1Level1s)
  private val level2sColumn        = IntColumnDef[TMClubDataPoint]("level2s", t => t.dcpData.p2Level2s)
  private val level2sAddColumn     = IntColumnDef[TMClubDataPoint]("level2s_add", t => t.dcpData.p3Level2sAdd)
  private val level3sColumn        = IntColumnDef[TMClubDataPoint]("level3s", t => t.dcpData.p4Level3s)
  private val level45dtmsColumn    = IntColumnDef[TMClubDataPoint]("level45dtms", t => t.dcpData.p5Level45D)
  private val level45dtmsAddColumn = IntColumnDef[TMClubDataPoint]("level45dtms_add", t => t.dcpData.p6Level45DAdd)
  private val newMembersColumn     = IntColumnDef[TMClubDataPoint]("new_members", t => t.dcpData.newMembers)
  private val addNewMembersColumn  = IntColumnDef[TMClubDataPoint]("add_new_members", t => t.dcpData.addNewMembers)
  private val officersTrainedRd1Column =
    IntColumnDef[TMClubDataPoint]("officers_trained_rd1", t => t.dcpData.officersTrainedRd1)
  private val officersTrainedRd2Column =
    IntColumnDef[TMClubDataPoint]("officers_trained_rd2", t => t.dcpData.officersTrainedRd2)
  private val cotMetColumn = BooleanColumnDef[TMClubDataPoint]("cot_met", t => t.dcpData.cotMet)
  private val membersDuesOnTimeOctColumn =
    BooleanColumnDef[TMClubDataPoint]("members_dues_on_time_oct", t => t.dcpData.memDuesOnTimeOct)
  private val membersDuesOnTimeAprColumn =
    BooleanColumnDef[TMClubDataPoint]("members_dues_on_time_apr", t => t.dcpData.memDuesOnTimeApr)
  private val officerListOnTimeColumn =
    BooleanColumnDef[TMClubDataPoint]("officer_list_on_time", t => t.dcpData.officerListOnTime)
  private val goal10MetColumn = BooleanColumnDef[TMClubDataPoint]("goal_10_met", t => t.dcpData.goal10Met)
  private val distinguishedStatusColumn =
    StringColumnDef[TMClubDataPoint]("distinguished_status", t => t.clubDistinctiveStatus, length = 2)
  private val membersGrowthColumn = IntColumnDef[TMClubDataPoint]("members_growth", t => t.membershipGrowth)
  private val awardsPerMemberColumn =
    DoubleColumnDef[TMClubDataPoint]("awards_per_member", t => t.awardsPerMember, df2dp)
  private val dcpEligibilityColumn = BooleanColumnDef[TMClubDataPoint]("dcp_eligibility", t => t.dcpEligibility)
  private val monthlyGrowthColumn  = IntColumnDef[TMClubDataPoint]("monthly_growth", t => t.monthlyGrowth)
  private val members30SeptColumn  = IntColumnDef[TMClubDataPoint]("members_30_sept", t => t.members30Sept)
  private val members31MarColumn   = IntColumnDef[TMClubDataPoint]("members_31_mar", t => t.members31Mar)
  private val regionColumn         = StringColumnDef[TMClubDataPoint]("region", t => t.region, length = 4)
  private val novAdVisitColumn = BooleanColumnDef[TMClubDataPoint]("nov_ad_visit", t => t.divData.exists(_.novADVisit))
  private val mayAdVisitColumn = BooleanColumnDef[TMClubDataPoint]("may_ad_visit", t => t.divData.exists(_.mayADVisit))
  private val totalNewMembersColumn =
    IntColumnDef[TMClubDataPoint]("total_new_members", t => t.distData.map(_.totalNewMembers).getOrElse(0))
  private val lateRenewalsColumn =
    IntColumnDef[TMClubDataPoint]("late_renewals", t => t.distData.map(_.lateRenewals).getOrElse(0))
  private val octRenewalsColumn =
    IntColumnDef[TMClubDataPoint]("oct_renewals", t => t.distData.map(_.octRenewals).getOrElse(0))
  private val aprRenewalsColumn =
    IntColumnDef[TMClubDataPoint]("apr_renewals", t => t.distData.map(_.aprRenewals).getOrElse(0))
  private val totalCharterColumn =
    IntColumnDef[TMClubDataPoint]("total_charter", t => t.distData.map(_.totalCharter).getOrElse(0))
  private val totalToDateColumn =
    IntColumnDef[TMClubDataPoint]("total_to_date", t => t.distData.map(_.totalToDate).getOrElse(0))
  private val charterSuspendDateColumn = StringColumnDef[TMClubDataPoint](
    "charter_suspend_date",
    t => t.distData.map(_.charterSuspendDate).getOrElse(""),
    length = 50
  )

  val columns: List[ColumnDef[TMClubDataPoint]] = List(
    programYearColumn,
    monthColumn,
    monthEndDateColumn,
    asOfDateColumn,
    districtColumn,
    divisionColumn,
    areaColumn,
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
    charterSuspendDateColumn
  )

  def latestDistrictMonthDates(ds: DataSource): Map[String, (Int, Int)] = {
    val allDistYearMonths = allDistrictYearMonths(ds)
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

  def existsByYearMonthDistrict(dataSource: DataSource, progYear: Int, month: Int, districtId: String): Boolean = {
    searchByDistrict(dataSource, districtId, Some(progYear), Some(month), Some(1)).nonEmpty
  }

  def allDistrictYearMonths(ds: DataSource): Seq[(String, Int, Int, LocalDate)] = {
    val listBuilder = List.newBuilder[(String, Int, Int, LocalDate)]
    ds.run(implicit conn => {
      conn.executeQuery(
        s"SELECT DISTINCT $districtColumnId, $programYearColumnId, $monthColumnId, $monthEndDateColumnId FROM $tableName",
        rs => {
          while (rs.next()) {
            listBuilder += ((rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getDate(4).toLocalDate))
          }
        }
      )
    })
    listBuilder.result()
  }

  def dcpDataFromResultSet(
      set: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubId: Int
  ): ClubDCPData = {
    ClubDCPData(
      programYear = programYear,
      month = month,
      asOfDate = asOfDate,
      clubId = clubId,
      oldCCs = ccsColumn.decode(set),
      oldCCsAdd = ccsAddColumn.decode(set),
      oldACs = acsColumn.decode(set),
      oldACsAdd = acsAddColumn.decode(set),
      oldLeaders = ldrColumn.decode(set),
      oldLeadersAdd = ldrsAddColumn.decode(set),
      p1Level1s = level1sColumn.decode(set),
      p2Level2s = level2sColumn.decode(set),
      p3Level2sAdd = level2sAddColumn.decode(set),
      p4Level3s = level3sColumn.decode(set),
      p5Level45D = level45dtmsColumn.decode(set),
      p6Level45DAdd = level45dtmsAddColumn.decode(set),
      officerListOnTime = officerListOnTimeColumn.decode(set),
      goal10Met = goal10MetColumn.decode(set),
      officersTrainedRd1 = officersTrainedRd1Column.decode(set),
      officersTrainedRd2 = officersTrainedRd2Column.decode(set),
      cotMet = cotMetColumn.decode(set),
      memDuesOnTimeApr = membersDuesOnTimeAprColumn.decode(set),
      memDuesOnTimeOct = membersDuesOnTimeOctColumn.decode(set),
      newMembers = newMembersColumn.decode(set),
      addNewMembers = addNewMembersColumn.decode(set)
    )
  }

  case class HDSearchKey(
      key: Option[String],
      district: Option[String],
      programYear: Option[Int],
      month: Option[Int],
      clubNumber: Option[Int]
  ) {
    def searchItems: List[SearchItem] = {
      List(
        district.map(d => SearchItem(districtColumnId, (stmt, idx) => stmt.setString(idx, d))),
        programYear.map(py => SearchItem(programYearColumnId, (stmt, idx) => stmt.setInt(idx, py))),
        month.map(m => SearchItem(monthColumn.name, (stmt, idx) => stmt.setInt(idx, m))),
        clubNumber.map(cn => SearchItem(clubNumberColumn.name, (stmt, idx) => stmt.setInt(idx, cn)))
      ).flatten
    }
  }

  private case class ValueSearch(searchKey: HDSearchKey) extends Search[TMClubDataPoint] {
    override def tableName: String             = HistoricClubPerfTableDef.tableName
    override def searchItems: List[SearchItem] = searchKey.searchItems
    override def columns: Option[List[String]] = None

    override def reader: ResultSet => TMClubDataPoint = read
  }

  def searchByDistrict(
      dataSource: DataSource,
      district: String,
      progYear: Option[Int] = None,
      month: Option[Int] = None,
      limit: Option[Int] = None
  ): List[TMClubDataPoint] = {
    val searchKey = HDSearchKey(None, Some(district), progYear, month, None)
    val search    = ValueSearch(searchKey)
    dataSource.run(implicit conn => {
      conn.search(search, limit)
    })
  }

  override def indexes: List[IndexDef[TMClubDataPoint]] = List(
    db.IndexDef("_club_year_month", this, List(programYearColumnId, monthColumnId, clubNumberColumnId), unique = true)
  )

  def findByClubYearMonth(
      clubNumber: Int,
      programYear: Int,
      month: Int,
      conn: Connection
  ): Option[TMClubDataPoint] = {
    val searchKey = HDSearchKey(None, None, Some(programYear), Some(month), Some(clubNumber))
    val search    = ValueSearch(searchKey)
    val res       = conn.search(search, limit = Some(1)).headOption
    res
  }

  def read(rs: java.sql.ResultSet): TMClubDataPoint = {
    val programYear  = rs.getInt(programYearColumnId)
    val month        = rs.getInt(monthColumnId)
    val asOfDate     = rs.getDate(asOfDateColumnId).toLocalDate
    val monthEndDate = rs.getDate(monthEndDateColumnId).toLocalDate
    val clubNumber   = rs.getInt("club_number")

    TMClubDataPoint(
      programYear,
      month,
      monthEndDate,
      asOfDate,
      rs.getString("district"),
      rs.getString("region"),
      rs.getString("division"),
      rs.getString("area"),
      clubNumber,
      rs.getString("club_name"),
      rs.getString("club_status"),
      rs.getInt("base_members"),
      rs.getInt("active_members"),
      rs.getInt("members_growth"),
      rs.getDouble("awards_per_member"),
      rs.getBoolean("dcp_eligibility"),
      rs.getInt("goals_met"),
      dcpDataFromResultSet(rs, programYear, month, asOfDate, clubNumber),
      rs.getString("distinguished_status"),
      rs.getInt("monthly_growth"),
      rs.getInt("members_30_sept"),
      rs.getInt("members_31_mar"),
      Some(readTMDivData(rs, programYear, month, asOfDate, clubNumber)),
      Some(readTMDistData(rs, programYear, month, asOfDate, clubNumber))
    )
  }

  private def readTMDistData(
      rs: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: Int
  ): TMDistClubDataPoint = {
    TMDistClubDataPoint(
      programYear,
      month,
      asOfDate,
      rs.getString("district"),
      rs.getString("division"),
      rs.getString("area"),
      clubNumber,
      rs.getString("club_name"),
      rs.getInt("total_new_members"),
      rs.getInt("late_renewals"),
      rs.getInt("oct_renewals"),
      rs.getInt("apr_renewals"),
      rs.getInt("total_charter"),
      rs.getInt("total_to_date"),
      rs.getString("charter_suspend_date")
    )
  }

  private def readTMDivData(
      rs: ResultSet,
      programYear: Int,
      month: Int,
      asOfDate: LocalDate,
      clubNumber: Int
  ): TMDivClubDataPoint = {
    TMDivClubDataPoint(
      programYear,
      month,
      asOfDate,
      rs.getString("district"),
      rs.getString("division"),
      rs.getString("area"),
      clubNumber,
      rs.getString("club_status"),
      rs.getInt("oct_renewals"),
      rs.getInt("apr_renewals"),
      rs.getBoolean("nov_ad_visit"),
      rs.getBoolean("may_ad_visit"),
      rs.getInt("active_members"),
      rs.getInt("goals_met"),
      rs.getString("distinguished_status")
    )
  }
}
