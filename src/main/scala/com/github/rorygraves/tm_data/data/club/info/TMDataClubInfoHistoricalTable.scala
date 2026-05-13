package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df5dp
import org.slf4j.LoggerFactory
import slick.collection.heterogeneous._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length
import slick.sql.SqlProfile.ColumnOption.NotNull

import java.time.{LocalDate, LocalDateTime}

/** Database table for storing historical snapshots of club information.
  *
  * Each record represents the state of a club at a specific point in time,
  * enabling temporal queries and audit trails of club data changes.
  * Records are immutable once created.
  */
class TMDataClubInfoHistoricalTable(val dbRunner: DBRunner) extends AbstractTable[ClubInfoHistoricalDataPoint] {

  private val logger    = LoggerFactory.getLogger("TMDataClubInfoHistoricalTable")
  def tableName: String = "club_details_historical"

  import slick.jdbc.PostgresProfile.api._

  class ClubInfoHistoricalTable(tag: Tag) extends Table[ClubInfoHistoricalDataPoint](tag, tableName) {
    def historyId        = column[Option[Long]]("history_id", O.PrimaryKey, O.AutoInc)
    def district         = column[String]("district", Length(3), NotNull)
    def division         = column[String]("division", Length(2), NotNull)
    def area             = column[String]("area", Length(3), NotNull)
    def clubNumber       = column[Int]("club_number", NotNull)
    def clubName         = column[String]("club_name")
    def charterDate      = column[Option[LocalDate]]("charter_date")
    def street           = column[Option[String]]("street")
    def city             = column[Option[String]]("city")
    def postcode         = column[Option[String]]("post_code", Length(20))
    def country          = column[Option[String]]("country")
    def location         = column[Option[String]]("location")
    def meetingTime      = column[Option[String]]("meeting_time")
    def meetingDay       = column[Option[String]]("meeting_day")
    def email            = column[Option[String]]("email")
    def phone            = column[Option[String]]("phone")
    def websiteLink      = column[Option[String]]("website_link")
    def facebookLink     = column[Option[String]]("facebook_link")
    def twitterLink      = column[Option[String]]("twitter_link")
    def latitude         = column[Double]("latitude")
    def longitude        = column[Double]("longitude")
    def advanced         = column[Boolean]("advanced")
    def prospective      = column[Boolean]("prospective")
    def onlineAttendance = column[Boolean]("online_attendance")
    def asOfDate         = column[LocalDateTime]("as_of_date", NotNull)

    def * = (historyId ::
      district ::
      division ::
      area ::
      clubNumber ::
      clubName ::
      charterDate ::
      street ::
      city ::
      postcode ::
      country ::
      location ::
      meetingTime ::
      meetingDay ::
      email ::
      phone ::
      websiteLink ::
      facebookLink ::
      twitterLink ::
      latitude ::
      longitude ::
      advanced ::
      prospective ::
      onlineAttendance ::
      asOfDate :: HNil).mapTo[ClubInfoHistoricalDataPoint]

    val idx1 = index(s"idx_${this.tableName}_club_date", (clubNumber, asOfDate), unique = false)
    val idx2 = index(s"idx_${this.tableName}_district", district, unique = false)
    val idx3 = index(s"idx_${this.tableName}_as_of_date", asOfDate, unique = false)
  }

  val tq = TableQuery[ClubInfoHistoricalTable]

  val columns: List[Column[ClubInfoHistoricalDataPoint]] = List(
    OptionalLongColumn("history_id", _.historyId),
    StringColumn("district", _.district),
    StringColumn("division", _.division),
    StringColumn("area", _.area),
    IntColumn("club_number", _.clubNumber),
    StringColumn("club_name", _.clubName),
    OptionalLocalDateColumn("charter_date", _.charterDate),
    OptionalStringColumn("street", _.street),
    OptionalStringColumn("city", _.city),
    OptionalStringColumn("post_code", _.postcode),
    OptionalStringColumn("country", _.country),
    OptionalStringColumn("location", _.location),
    OptionalStringColumn("meeting_time", _.meetingTime),
    OptionalStringColumn("meeting_day", _.meetingDay),
    OptionalStringColumn("email", _.email),
    OptionalStringColumn("phone", _.phone),
    OptionalStringColumn("website_link", _.websiteLink),
    OptionalStringColumn("facebook_link", _.facebookLink),
    OptionalStringColumn("twitter_link", _.twitterLink),
    DoubleColumn("latitude", _.latitude, formatter = df5dp),
    DoubleColumn("longitude", _.longitude, formatter = df5dp),
    BooleanColumn("advanced", _.advanced),
    BooleanColumn("prospective", _.prospective),
    BooleanColumn("online_attendance", _.onlineAttendance),
    LocalDateTimeColumn("as_of_date", _.asOfDate)
  )

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    logger.debug("TMDataClubInfoHistoricalTable.createIfNotExists")
    createTableFromStatements(statements)
  }

  def insertHistoricalClubInfo(row: ClubInfoHistoricalDataPoint): Unit = {
    dbRunner.dbAwait((tq += row).transactionally)
  }

  def insertHistoricalClubInfos(rows: Seq[ClubInfoHistoricalDataPoint]): Unit = {
    dbRunner.dbAwait((tq ++= rows).transactionally)
  }

  def getClubHistory(clubNumber: Int): Seq[ClubInfoHistoricalDataPoint] = {
    dbRunner.dbAwait(
      tq.filter(_.clubNumber === clubNumber)
        .sortBy(_.asOfDate.desc)
        .result
    )
  }

  def getClubHistoryBetween(clubNumber: Int, startDate: LocalDateTime, endDate: LocalDateTime): Seq[ClubInfoHistoricalDataPoint] = {
    dbRunner.dbAwait(
      tq.filter(h => h.clubNumber === clubNumber && h.asOfDate >= startDate && h.asOfDate <= endDate)
        .sortBy(_.asOfDate.desc)
        .result
    )
  }

  def getLatestHistoricalEntry(clubNumber: Int): Option[ClubInfoHistoricalDataPoint] = {
    dbRunner.dbAwait(
      tq.filter(_.clubNumber === clubNumber)
        .sortBy(_.asOfDate.desc)
        .result
        .headOption
    )
  }

  def getAllClubsAtDate(asOfDate: LocalDateTime): Seq[ClubInfoHistoricalDataPoint] = {
    dbRunner.dbAwait(
      tq.filter(_.asOfDate === asOfDate).result
    )
  }

  /** Create a historical snapshot from current club data.
    *
    * Transforms a current ClubInfoDataPoint into a historical record
    * with the specified timestamp. Used when changes are detected.
    *
    * @param currentData The current club information to snapshot
    * @param asOfDate The timestamp for this snapshot
    * @return A new historical data point ready for insertion
    */
  def createSnapshotFromCurrent(
      currentData: ClubInfoDataPoint,
      asOfDate: LocalDateTime = LocalDateTime.now()
  ): ClubInfoHistoricalDataPoint = {
    ClubInfoHistoricalDataPoint(
      historyId = None,
      district = currentData.district,
      division = currentData.division,
      area = currentData.area,
      clubNumber = currentData.clubNumber,
      clubName = currentData.clubName,
      charterDate = currentData.charterDate,
      street = currentData.street,
      city = currentData.city,
      postcode = currentData.postcode,
      country = currentData.country,
      location = currentData.location,
      meetingTime = currentData.meetingTime,
      meetingDay = currentData.meetingDay,
      email = currentData.email,
      phone = currentData.phone,
      websiteLink = currentData.websiteLink,
      facebookLink = currentData.facebookLink,
      twitterLink = currentData.twitterLink,
      latitude = currentData.latitude,
      longitude = currentData.longitude,
      advanced = currentData.advanced,
      prospective = currentData.prospective,
      onlineAttendance = currentData.onlineAttendance,
      asOfDate = asOfDate
    )
  }
}
