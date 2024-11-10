package app.data.club.info

import java.time.LocalDate

case class ClubInfoDataPoint(
    clubId: Int,
    clubName: String,
    district: String,
    division: String,
    area: String,
    prospective: Boolean,
    street: String,
    city: String,
    postcode: String,
    longitude: Double,
    latitude: Double,
    countryName: String,
    charterDate: Option[LocalDate],
    email: String,
    location: String,
    meetingDay: String,
    meetingTime: String,
    phone: String,
    facebookLink: String,
    website: String,
    onlineAttendance: Boolean
)
