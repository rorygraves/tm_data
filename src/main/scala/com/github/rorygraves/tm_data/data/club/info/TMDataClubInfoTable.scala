package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df5dp
import slick.collection.heterogeneous._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length
import slick.sql.SqlProfile.ColumnOption.NotNull

import java.time.LocalDate

class TMDataClubInfoTable(dbRunner: DBRunner) extends TableDef[ClubInfoDataPoint] {

  import slick.jdbc.PostgresProfile.api._

  class ClubInfoTable(tag: Tag) extends Table[ClubInfoDataPoint](tag, "club_details") {
    def district         = column[String]("district", Length(3), NotNull)
    def division         = column[String]("division", Length(2), NotNull)
    def area             = column[String]("area", Length(3), NotNull)
    def clubNumber       = column[Int]("club_number", O.PrimaryKey)
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

    def * = (district ::
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
      onlineAttendance :: HNil).mapTo[ClubInfoDataPoint]

    val idx1 = index(s"idx_${this.tableName}_district", district, unique = false)

  }

  val tq = TableQuery[ClubInfoTable]

  private val clubNumberColumnId = "club_number"

  override def tableName: String = "club_details"

  private val districtColumn    = StringColumnDef[ClubInfoDataPoint]("district", _.district, length = 3)
  private val divisionColumn    = StringColumnDef[ClubInfoDataPoint]("division", _.division, length = 2)
  private val areaColumn        = StringColumnDef[ClubInfoDataPoint]("area", _.area, length = 3)
  private val clubNumberColumn  = IntColumnDef[ClubInfoDataPoint](clubNumberColumnId, _.clubNumber, primaryKey = true)
  private val clubNameColumn    = StringColumnDef[ClubInfoDataPoint]("club_name", _.clubName)
  private val charterDateColumn = OptionalLocalDateColumnDef[ClubInfoDataPoint]("charter_date", _.charterDate)

  private val streetColumn   = OptionalStringColumnDef[ClubInfoDataPoint]("street", _.street)
  private val cityColumn     = OptionalStringColumnDef[ClubInfoDataPoint]("city", _.city)
  private val postcodeColumn = OptionalStringColumnDef[ClubInfoDataPoint]("post_code", _.postcode, length = 20)

  private val countryColumn     = OptionalStringColumnDef[ClubInfoDataPoint]("country", _.country)
  private val locationColumn    = OptionalStringColumnDef[ClubInfoDataPoint]("location", _.location)
  private val meetingTimeColumn = OptionalStringColumnDef[ClubInfoDataPoint]("meeting_time", _.meetingTime)
  private val meetingDayColumn  = OptionalStringColumnDef[ClubInfoDataPoint]("meeting_day", _.meetingDay)

  private val emailColumn = OptionalStringColumnDef[ClubInfoDataPoint]("email", _.email)

  private val phoneColumn            = OptionalStringColumnDef[ClubInfoDataPoint]("phone", _.phone)
  private val websiteColumn          = OptionalStringColumnDef[ClubInfoDataPoint]("website_link", _.websiteLink)
  private val facebookColumn         = OptionalStringColumnDef[ClubInfoDataPoint]("facebook_link", _.facebookLink)
  private val twitterColumn          = OptionalStringColumnDef[ClubInfoDataPoint]("twitter_link", _.twitterLink)
  private val latitudeColumn         = DoubleColumnDef[ClubInfoDataPoint]("latitude", _.latitude, formatter = df5dp)
  private val longitudeColumn        = DoubleColumnDef[ClubInfoDataPoint]("longitude", _.longitude, formatter = df5dp)
  private val advancedColumn         = BooleanColumnDef[ClubInfoDataPoint]("advanced", _.advanced)
  private val prospectiveColumn      = BooleanColumnDef[ClubInfoDataPoint]("prospective", _.prospective)
  private val onlineAttendanceColumn = BooleanColumnDef[ClubInfoDataPoint]("online_attendance", _.onlineAttendance)

  override val columns: List[ColumnDef[ClubInfoDataPoint]] = List(
    districtColumn,
    divisionColumn,
    areaColumn,
    clubNumberColumn,
    clubNameColumn,
    charterDateColumn,
    streetColumn,
    cityColumn,
    postcodeColumn,
    countryColumn,
    locationColumn,
    meetingTimeColumn,
    meetingDayColumn,
    emailColumn,
    phoneColumn,
    websiteColumn,
    facebookColumn,
    twitterColumn,
    latitudeColumn,
    longitudeColumn,
    advancedColumn,
    prospectiveColumn,
    onlineAttendanceColumn
  )

  def allClubInfo(districtId: String): List[ClubInfoDataPoint] = {
    dbRunner.dbAwait(tq.filter(_.district === districtId).result).toList
  }

  def createIfNotExists(): Unit = {
    dbRunner.dbAwait(tq.schema.createIfNotExists)
  }

  def insertClubInfos(newRows: Seq[ClubInfoDataPoint]): Unit = {
    dbRunner.dbAwait((tq ++= newRows).transactionally)
  }

  def updateClubInfos(newRows: Seq[ClubInfoDataPoint]): Unit = {

    println("Update club infos")
    newRows.foreach(println)
    dbRunner.dbAwait(DBIO.seq(newRows.map(tq.insertOrUpdate): _*).transactionally)
  }

  def removeClubInfos(ids: List[Int]): Unit = {

    dbRunner.dbAwait(tq.filter(_.clubNumber inSet ids).delete.transactionally)
  }
}
