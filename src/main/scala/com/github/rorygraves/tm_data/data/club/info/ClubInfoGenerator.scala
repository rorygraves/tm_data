package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.util.DBRunner
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters.IterableHasAsJava
import java.io.{File, PrintWriter, StringWriter}
import java.time.{Instant, LocalDate, ZoneId}

/** Utility to generate club information from the TI Club search (e.g. locations etc */
class ClubInfoGenerator(clubInfoTableDef: TMDataClubInfoTable) {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  import slick.jdbc.PostgresProfile.api._

  def generateClubData(districtId: String): Int = {
    val downloadedClubs = downloadClubInfoFromTM(districtId)

    val currentRows = clubInfoTableDef.allClubInfo(districtId)

    // find new rows in downloaded data and not in currentRows
    val newRows = downloadedClubs.filterNot { downloadedRow =>
      currentRows.exists { currentRow =>
        currentRow.clubNumber == downloadedRow.clubNumber
      }
    }
    println(s"District $districtId - generateClubData - found ${newRows.length} new rows")
    clubInfoTableDef.insertClubInfos(newRows)

    // find rows that have been updated in the downloadedClubs data
    val updatedRows = downloadedClubs.filter { downloadedRow =>
      currentRows.exists { currentRow =>
        currentRow.clubNumber == downloadedRow.clubNumber && currentRow != downloadedRow
      }
    }

    logger.info(s"District $districtId - generateClubData - found ${updatedRows.length} updated rows")
    clubInfoTableDef.updateClubInfos(updatedRows)

    // find the rows that are in the current data but not in the downloaded data
    val deletedRows = currentRows.filterNot { currentRow =>
      downloadedClubs.exists { downloadedRow =>
        currentRow.clubNumber == downloadedRow.clubNumber
      }
    }

    clubInfoTableDef.removeClubInfos(deletedRows.map(_.clubNumber))

    println(s"District $districtId - generateClubData - found ${deletedRows.length} deleted rows")

    outputClubData(downloadedClubs, districtId)
    downloadedClubs.size
  }

  def outputClubData(data: Seq[ClubInfoDataPoint], districtId: String): Unit = {
    // output results to CSV
    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)
    try {

      // output the headers
      printer.printRecord(clubInfoTableDef.columns.map(_.name).asJava)
      // output the rows
      data.foreach { point =>
        val rowValues = clubInfoTableDef.columns.map(c => c.csvExportFn(point))
        printer.printRecord(rowValues.asJava)
      }

      val writer = new PrintWriter(new File(s"data/club_info_$districtId.csv"))
      writer.write(out.toString)
      writer.close()

    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
    } finally if (printer != null) printer.close()

  }

  def downloadClubInfoFromTM(districtId: String): Seq[ClubInfoDataPoint] = {
    logger.info(s"Downloading club info for district $districtId")
    val url =
      s"https://www.toastmasters.org/api/sitecore/FindAClub/Search?q=&district=$districtId&advanced=1&latitude=1&longitude=1"
    val r    = requests.get(url)
    val json = ujson.read(r.bytes)

    def parseDate(dateString: String): LocalDate = {
      val epochTime = dateString.stripPrefix("/Date(").stripSuffix(")/").toLong
      Instant.ofEpochMilli(epochTime).atZone(ZoneId.systemDefault()).toLocalDate
    }

    json("Clubs").arr.map { clubJson =>
      try {
//        println(ujson.write(clubJson, indent = 4))

        val address = clubJson("Address")
        ClubInfoDataPoint(
          district = clubJson("Classification")("District")("Name").str,
          division = clubJson("Classification")("Division")("Name").str,
          area = clubJson("Classification")("Area")("Name").str,
          clubNumber = clubJson("Identification")("Id")("Value").str.toInt,
          clubName = clubJson("Identification")("Id")("Name").str,
          charterDate = clubJson("CharterDate").strOpt.map(parseDate),
          street = address("Street").strOpt,
          city = address("City").strOpt,
          postcode = address("PostalCode").strOpt,
          country = clubJson("CountryName").strOpt,
          location = clubJson("Location").strOpt,
          meetingTime = clubJson("MeetingTime").strOpt,
          meetingDay = clubJson("MeetingDay").strOpt,
          email = clubJson("Email").strOpt,
          phone = clubJson("Phone").strOpt,
          websiteLink = clubJson("Website").strOpt,
          facebookLink = clubJson("FacebookLink").strOpt,
          twitterLink = clubJson("TwitterLink").strOpt,
          longitude = address("Coordinates")("Longitude").num,
          latitude = address("Coordinates")("Latitude").num,
          onlineAttendance = clubJson("AllowsVirtualAttendance").bool,
          prospective = clubJson("IsProspective").bool,
          advanced = false
        )
      } catch {
        case e: Exception =>
          println("Error parsing club data")
          println(ujson.write(clubJson, indent = 4))
          throw e
      }
    }.toSeq
  }

}
