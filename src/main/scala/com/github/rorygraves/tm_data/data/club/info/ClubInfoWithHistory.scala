package com.github.rorygraves.tm_data.data.club.info

import com.github.rorygraves.tm_data.util.DBRunner
import slick.jdbc.PostgresProfile.api._
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import scala.concurrent.ExecutionContext

/** Manages club information with automatic historical tracking.
  *
  * This class provides change detection and automatic versioning of club data.
  * When club information changes, it creates historical snapshots before updating
  * the current data, enabling time-based queries and audit trails.
  */
class ClubInfoWithHistory(
    val clubInfoTable: TMDataClubInfoTable,
    val historicalTable: TMDataClubInfoHistoricalTable,
    val dbRunner: DBRunner
) {

  private val logger                        = LoggerFactory.getLogger("ClubInfoWithHistory")
  private implicit val ec: ExecutionContext = ExecutionContext.global

  /** Update club information with automatic history tracking.
    *
    * This method:
    * 1. Detects changes by comparing new data with existing records
    * 2. Creates historical snapshots for changed or new clubs
    * 3. Updates the current club information table
    *
    * All operations are performed in a single transaction for consistency.
    *
    * @param newRows Updated club information to save
    * @param asOfDate Timestamp for the historical snapshot (defaults to now)
    */
  def updateClubInfosWithHistory(newRows: Seq[ClubInfoDataPoint], asOfDate: LocalDateTime = LocalDateTime.now()): Unit = {
    dbRunner.dbAwait(
      (for {
        // Get existing records to detect changes
        existingClubs <- clubInfoTable.tq
          .filter(_.clubNumber inSet newRows.map(_.clubNumber))
          .result

        // Create historical snapshots for clubs that have changed
        historicalSnapshots = newRows.flatMap { newClub =>
          existingClubs.find(_.clubNumber == newClub.clubNumber) match {
            case Some(oldClub) if hasClubChanged(oldClub, newClub) =>
              // Club has changed - create a historical snapshot of the NEW state
              Some(historicalTable.createSnapshotFromCurrent(newClub, asOfDate))
            case None =>
              // New club - create initial historical snapshot
              Some(historicalTable.createSnapshotFromCurrent(newClub, asOfDate))
            case _ =>
              // No changes detected
              None
          }
        }

        // Insert historical snapshots if any changes detected
        _ <-
          if (historicalSnapshots.nonEmpty) {
            historicalTable.tq ++= historicalSnapshots
          } else {
            DBIO.successful(())
          }

        // Update current club info
        _ <- DBIO.seq(newRows.map(clubInfoTable.tq.insertOrUpdate): _*)

      } yield ()).transactionally
    )

    logger.info(s"Updated ${newRows.size} clubs with history tracking")
  }

  /** Detect if any tracked club attributes have changed.
    * Compares all relevant fields to determine if a historical snapshot is needed.
    */
  private def hasClubChanged(oldClub: ClubInfoDataPoint, newClub: ClubInfoDataPoint): Boolean = {
    oldClub.district != newClub.district ||
    oldClub.division != newClub.division ||
    oldClub.area != newClub.area ||
    oldClub.clubName != newClub.clubName ||
    oldClub.charterDate != newClub.charterDate ||
    oldClub.street != newClub.street ||
    oldClub.city != newClub.city ||
    oldClub.postcode != newClub.postcode ||
    oldClub.country != newClub.country ||
    oldClub.location != newClub.location ||
    oldClub.meetingTime != newClub.meetingTime ||
    oldClub.meetingDay != newClub.meetingDay ||
    oldClub.email != newClub.email ||
    oldClub.phone != newClub.phone ||
    oldClub.websiteLink != newClub.websiteLink ||
    oldClub.facebookLink != newClub.facebookLink ||
    oldClub.twitterLink != newClub.twitterLink ||
    oldClub.latitude != newClub.latitude ||
    oldClub.longitude != newClub.longitude ||
    oldClub.advanced != newClub.advanced ||
    oldClub.prospective != newClub.prospective ||
    oldClub.onlineAttendance != newClub.onlineAttendance
  }

  def getClubWithHistory(clubNumber: Int): Option[(ClubInfoDataPoint, Seq[ClubInfoHistoricalDataPoint])] = {
    clubInfoTable.get(clubNumber).map { club =>
      val history = historicalTable.getClubHistory(clubNumber)
      (club, history)
    }
  }

  def getClubAtPointInTime(clubNumber: Int, pointInTime: LocalDateTime): Option[ClubInfoHistoricalDataPoint] = {
    // Get the most recent historical snapshot before or at the specified time
    dbRunner.dbAwait(
      historicalTable.tq
        .filter(h => h.clubNumber === clubNumber && h.asOfDate <= pointInTime)
        .sortBy(_.asOfDate.desc)
        .result
        .headOption
    )
  }
}
