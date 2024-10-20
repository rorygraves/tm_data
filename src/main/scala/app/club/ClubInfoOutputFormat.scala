package app.club

import app.Main.decimalFormatter

object ClubInfoOutputFormat {

  case class ColumnDef(
      name: String,
      calculation: ClubInfoDataPoint => String
  )

  val clubColumnGenerator: List[ColumnDef] = List[ColumnDef](
    ColumnDef("ClubId", t => t.clubId.toString),
    ColumnDef("ClubName", t => t.clubName),
    ColumnDef("District", t => t.district),
    ColumnDef("Division", t => t.division),
    ColumnDef("Area", t => t.area),
    ColumnDef("Prospective", t => t.prospective.toString),
    ColumnDef("Street", t => t.street),
    ColumnDef("City", t => t.city),
    ColumnDef("Postcode", t => t.postcode),
    ColumnDef("Longitude", t => decimalFormatter.format(t.longitude)),
    ColumnDef("Latitude", t => decimalFormatter.format(t.latitude)),
    ColumnDef("CountryName", t => t.countryName),
    ColumnDef("CharterDate", t => t.charterDate.map(_.toString).getOrElse("")),
    // disabled for now
    //    ColumnDef("Email", t => t.email),
    //    ColumnDef("Phone", t => t.phone),
    ColumnDef("Location", t => t.location),
    ColumnDef("MeetingDay", t => t.meetingDay),
    ColumnDef("MeetingTime", t => t.meetingTime),
    ColumnDef("FacebookLink", t => t.facebookLink),
    ColumnDef("Website", t => t.website)
  )

}
