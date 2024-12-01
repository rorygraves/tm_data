package app.data.club.info

import app.db._
import app.util.FormatUtil.df5dp

import java.sql.ResultSet

object ClubInfoTableDef extends TableDef[ClubInfoDataPoint] {

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

  def fromResultSet(rs: ResultSet): ClubInfoDataPoint = {
    ClubInfoDataPoint(
      district = districtColumn.decode(rs),
      division = divisionColumn.decode(rs),
      area = areaColumn.decode(rs),
      clubNumber = clubNumberColumn.decode(rs),
      clubName = clubNameColumn.decode(rs),
      charterDate = charterDateColumn.decode(rs),
      street = streetColumn.decode(rs),
      city = cityColumn.decode(rs),
      postcode = postcodeColumn.decode(rs),
      country = countryColumn.decode(rs),
      location = locationColumn.decode(rs),
      meetingTime = meetingTimeColumn.decode(rs),
      meetingDay = meetingDayColumn.decode(rs),
      email = emailColumn.decode(rs),
      phone = phoneColumn.decode(rs),
      websiteLink = websiteColumn.decode(rs),
      facebookLink = facebookColumn.decode(rs),
      twitterLink = twitterColumn.decode(rs),
      latitude = latitudeColumn.decode(rs),
      longitude = longitudeColumn.decode(rs),
      advanced = advancedColumn.decode(rs),
      prospective = prospectiveColumn.decode(rs),
      onlineAttendance = onlineAttendanceColumn.decode(rs)
    )
  }

  def allClubInfo(districtId: String, dataSource: DataSource): List[ClubInfoDataPoint] = {
    dataSource.run { implicit conn =>
      conn.executeQuery(
        s"SELECT * FROM $tableName WHERE district = '$districtId'",
        { rs =>
          val listBuilder = List.newBuilder[ClubInfoDataPoint]
          while (rs.next()) {
            listBuilder += fromResultSet(rs)
          }
          listBuilder.result()
        }
      )
    }
  }

  def insertClubInfos(newRows: Seq[ClubInfoDataPoint], dataSource: DataSource): Unit =
    dataSource.transaction { implicit conn =>
      newRows.foreach { row =>
        conn.insert(row, ClubInfoTableDef)
      }
    }

  def updateClubInfos(newRows: Seq[ClubInfoDataPoint], dataSource: DataSource): Unit = {
    dataSource.transaction { implicit conn =>
      newRows.foreach { row =>
        conn.update(row, ClubInfoTableDef)
      }
    }
  }

  def removeClubInfos(ints: List[Int], dataSource: DataSource): Unit = {
    dataSource.transaction { implicit conn =>
      ints.map { clubNumber =>
        conn.executeUpdate(s"DELETE FROM $tableName WHERE $clubNumberColumnId = $clubNumber")
      }.sum
    }
  }

}
