package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.util.FormatUtil.df5dp
import slick.collection.heterogeneous._
import slick.lifted.ProvenShape.proveShapeOf
import slick.relational.RelationalProfile.ColumnOption.Length
import slick.sql.SqlProfile.ColumnOption.NotNull

import java.time.LocalDate

class TMDataClubInfoTable(val dbRunner: DBRunner) extends AbstractTable[ClubInfoDataPoint] {

  def tableName: String = "club_details"

  import slick.jdbc.PostgresProfile.api._

  class ClubInfoTable(tag: Tag) extends Table[ClubInfoDataPoint](tag, tableName) {
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

  val columns: List[Column[ClubInfoDataPoint]] = List(
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
    BooleanColumn("online_attendance", _.onlineAttendance)
  )

  def allClubInfo(districtId: String): List[ClubInfoDataPoint] = {
    dbRunner.dbAwait(tq.filter(_.district === districtId).result).toList
  }

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    println("TMDataClubInfoTable.createIfNotExists-------------------------------------------")
    createTableFromStatements(statements)
  }

  def insertClubInfos(newRows: Seq[ClubInfoDataPoint]): Unit = {
    dbRunner.dbAwait((tq ++= newRows).transactionally)
  }

  def updateClubInfos(newRows: Seq[ClubInfoDataPoint]): Unit = {
    dbRunner.dbAwait(DBIO.seq(newRows.map(tq.insertOrUpdate): _*).transactionally)
  }

  def removeClubInfos(ids: List[Int]): Unit = {
    dbRunner.dbAwait(tq.filter(_.clubNumber inSet ids).delete.transactionally)
  }

  def get(clubId: Int): Option[ClubInfoDataPoint] =
    dbRunner.dbAwait(tq.filter(_.clubNumber === clubId).result.headOption)
}
