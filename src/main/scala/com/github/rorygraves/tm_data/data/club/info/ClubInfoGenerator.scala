package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.util.DistrictUtil
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

/** Utility to generate club information from the TI Club search (e.g. locations etc */
class ClubInfoGenerator(clubInfoWithHistory: ClubInfoWithHistory) {

  val logger: Logger = LoggerFactory.getLogger("ClubInfoGenerator")

  // For backward compatibility, expose the table
  private val clubInfoTableDef = clubInfoWithHistory.clubInfoTable

  def generateClubData(districtId: String): Int = {
    val downloadedClubs = downloadClubInfoFromTM(districtId)
    val asOfDate        = LocalDateTime.now()

    val currentRows = clubInfoTableDef.allClubInfo(districtId)

    // find new rows in downloaded data and not in currentRows
    val newRows = downloadedClubs.filterNot { downloadedRow =>
      currentRows.exists { currentRow =>
        currentRow.clubNumber == downloadedRow.clubNumber
      }
    }

    // find rows that have been updated in the downloadedClubs data
    val updatedRows = downloadedClubs.filter { downloadedRow =>
      currentRows.exists { currentRow =>
        currentRow.clubNumber == downloadedRow.clubNumber && currentRow != downloadedRow
      }
    }

    // Combine new and updated rows for history tracking
    val rowsToUpdate = newRows ++ updatedRows

    if (rowsToUpdate.nonEmpty) {
      // Use the history-aware update method which will:
      // 1. Create historical snapshots for changed clubs
      // 2. Update the current club info table
      clubInfoWithHistory.updateClubInfosWithHistory(rowsToUpdate, asOfDate)
    }

    // find the rows that are in the current data but not in the downloaded data
    val deletedRows = currentRows.filterNot { currentRow =>
      downloadedClubs.exists { downloadedRow =>
        currentRow.clubNumber == downloadedRow.clubNumber
      }
    }

    if (deletedRows.nonEmpty) {
      // For deleted clubs, we should still track them in history before removal
      // Create final historical snapshots with a deletion marker (we can use the current data)
      val deletionSnapshots = deletedRows.map { club =>
        clubInfoWithHistory.historicalTable.createSnapshotFromCurrent(club, asOfDate)
      }
      clubInfoWithHistory.historicalTable.insertHistoricalClubInfos(deletionSnapshots)

      // Now remove from current table
      clubInfoTableDef.removeClubInfos(deletedRows.map(_.clubNumber))
    }

    logger.info(
      s"District $districtId clubInfo: ${newRows.length} new, ${updatedRows.length} updated, ${deletedRows.length} deleted"
    )

    downloadedClubs.size
  }

  def downloadClubInfoFromTM(districtId: String): Seq[ClubInfoDataPoint] = {
    val apiDistrictId = DistrictUtil.padDistrictId(districtId)
    logger.debug(s"Downloading club info for district $districtId using TI district parameter $apiDistrictId")
    // https://www.toastmasters.org/api/sitecore/FindAClub/DownloadCsv?longitude=1&latitude=1&district=91&advanced=1
    val url =
      s"https://www.toastmasters.org/api/sitecore/FindAClub/Search?q=&district=$apiDistrictId&advanced=1&latitude=1&longitude=1"
    val r    = requests.get(url)
    val json = ujson.read(r.bytes)

    def parseDate(dateString: String): LocalDate = {
      val epochTime = dateString.stripPrefix("/Date(").stripSuffix(")/").toLong
      Instant.ofEpochMilli(epochTime).atZone(ZoneId.systemDefault()).toLocalDate
    }

    json("Clubs").arr.map { clubJson =>
      try {

        val clubNumber = clubJson("Identification")("Id")("Value").str.toInt
        val clubName   = clubJson("Identification")("Id")("Name").str

        if (clubName.contains("Battle"))
          logger.debug("Club JSON: " + ujson.write(clubJson, indent = 2))
        val address = clubJson("Address")
        ClubInfoDataPoint(
          district = DistrictUtil.cleanDistrict(clubJson("Classification")("District")("Name").str),
          division = clubJson("Classification")("Division")("Name").str,
          area = DistrictUtil.cleanDistrict(clubJson("Classification")("Area")("Name").str),
          clubNumber = clubNumber,
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
          logger.error("Error parsing club data")
          logger.error(ujson.write(clubJson, indent = 4))
          throw e
      }
    }.toSeq
  }

}
