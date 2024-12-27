package com.github.rorygraves.tm_data.data.club.info

import java.time.LocalDate

case class ClubInfoDataPoint(
    district: String,
    division: String,
    area: String,
    clubNumber: Int,
    clubName: String,
    charterDate: Option[LocalDate],
    street: Option[String],
    city: Option[String],
    postcode: Option[String],
    country: Option[String],
    location: Option[String],
    meetingTime: Option[String],
    meetingDay: Option[String],
    email: Option[String],
    phone: Option[String],
    websiteLink: Option[String],
    facebookLink: Option[String],
    twitterLink: Option[String],
    latitude: Double,
    longitude: Double,
    advanced: Boolean,
    prospective: Boolean,
    onlineAttendance: Boolean
)
