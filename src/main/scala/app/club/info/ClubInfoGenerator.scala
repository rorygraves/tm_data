package app.club.info

import app.club.info.ClubInfoOutputFormat.clubColumnGenerator
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.jdk.CollectionConverters.IterableHasAsJava
import java.io.{File, PrintWriter, StringWriter}
import java.time.{Instant, LocalDate, ZoneId}

/** Utility to generate club information from the TI Club search (e.g. locations etc */
object ClubInfoGenerator {

  def generateClubData(districtId: Int): Unit = {
    val clubs = fetchClubData(districtId)
    outputClubData(clubs, districtId)
  }

  def outputClubData(data: Seq[ClubInfoDataPoint], districtId: Int): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      // output the headers
      printer.printRecord(clubColumnGenerator.map(_.name).asJava)
      // output the rows
      data.foreach { point =>
        val rowValues = clubColumnGenerator.map(_.calculation(point))
        printer.printRecord(rowValues.asJava)
      }

      // log the output for debug
      println(out.toString.take(1500))

      val writer = new PrintWriter(new File(s"data/club_info_$districtId.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()

  }

  def fetchClubData(districtId: Int): Seq[ClubInfoDataPoint] = {
    val url =
      s"https://www.toastmasters.org/api/sitecore/FindAClub/Search?q=&district=$districtId&advanced=1&latitude=1&longitude=1"
    val r    = requests.get(url)
    val json = ujson.read(r.bytes)

    def parseDate(dateString: String): LocalDate = {
      val epochTime = dateString.stripPrefix("/Date(").stripSuffix(")/").toLong
      Instant.ofEpochMilli(epochTime).atZone(ZoneId.systemDefault()).toLocalDate
    }

    json("Clubs").arr.map { clubJson =>
      println("-------------------------")
      println(ujson.write(clubJson, indent = 4))
      val address = clubJson("Address")
      ClubInfoDataPoint(
        clubId = clubJson("Identification")("Id")("Value").str.toInt,
        clubName = clubJson("Identification")("Id")("Name").str,
        district = clubJson("Classification")("District")("Name").str,
        division = clubJson("Classification")("Division")("Name").str,
        area = clubJson("Classification")("Area")("Name").str,
        prospective = clubJson("IsProspective").bool,
        street = address("Street").strOpt.getOrElse(""),
        city = address("City").strOpt.getOrElse(""),
        postcode = address("PostalCode").strOpt.getOrElse(""),
        longitude = address("Coordinates")("Longitude").num,
        latitude = address("Coordinates")("Latitude").num,
        countryName = clubJson("CountryName").strOpt.getOrElse(""),
        charterDate = clubJson("CharterDate").strOpt.map(parseDate),
        email = clubJson("Email").strOpt.getOrElse(""),
        location = clubJson("Location").strOpt.getOrElse(""),
        meetingDay = clubJson("MeetingDay").strOpt.getOrElse(""),
        meetingTime = clubJson("MeetingTime").strOpt.getOrElse(""),
        phone = clubJson("Phone").strOpt.getOrElse(""),
        facebookLink = clubJson("FacebookLink").strOpt.getOrElse(""),
        website = clubJson("Website").strOpt.getOrElse(""),
        onlineAttendance = clubJson("AllowsVirtualAttendance").bool
      )
    }.toSeq
  }

}
