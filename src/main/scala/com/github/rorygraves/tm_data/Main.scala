package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, ClubInfoTableDef}
import com.github.rorygraves.tm_data.data.club.perf.historical.HistoricClubPerfGenerator
import com.github.rorygraves.tm_data.data.club.perf.historical.data.HistoricClubPerfTableDef
import com.github.rorygraves.tm_data.data.district.historical.{
  DistrictSummaryHistoricalGenerator,
  DistrictSummaryHistoricalTableDef
}
import com.github.rorygraves.tm_data.util.{DBRunner, FixedDBRunner}
import org.slf4j.LoggerFactory
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object Main {

  val logger              = LoggerFactory.getLogger(getClass)
  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  // 72 , 129

  def generateDistrictData(
      districtId: String,
      progStartYear: Int,
      progStartMonth: Int,
      dbRunner: DBRunner
  ): Unit = {
//
//    ClubInfoGenerator.generateClubData(districtId, database)

    HistoricClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      progStartYear,
      progStartMonth,
      dbRunner
    )

  }

  def main(args: Array[String]): Unit = {

//    val config: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig[JdbcProfile]("tm_data")
    val db       = Database.forConfig("tm_data")
    val dbRunner = new FixedDBRunner(db)
    try {
//      DistrictSummaryHistoricalTableDef.allDistrictYearMonths(db).foreach(println)
      runMainImport(db, dbRunner)
//      ClubInfoTableDef.createIfNotExists(db)
//      val districtId = "91"
//      val clubs      = ClubInfoTableDef.allClubInfo(districtId, db)
//      println("Clubs.size = " + clubs.size)
//      println("Generating club info for district " + districtId)
//      ClubInfoGenerator.generateClubData(districtId, db)

    } catch {
      case e: Exception =>
        println("Exception during main import")
        e.printStackTrace()
        System.exit(1)

    } finally db.close

  }
  def runMainImport(database: Database, dbRunner: DBRunner): Unit = {

//    ds.run(implicit conn => {
//      println("Ensuring club info table exists")
//      conn.create(ClubInfoTableDef)
//      println("Ensuring historic club performance table exists")
//      conn.create(HistoricClubPerfTableDef)
//      println("Ensuring district summary historical table exists")
//      conn.create(DistrictSummaryHistoricalTableDef)
//    })

//    println("Generating historical overview data")
//
//    DistrictSummaryHistoricalGenerator.generateHistoricalOverviewData(cacheFolder, database)

    logger.info("Fetching district Ids")
    val allDistrictIds = DistrictSummaryHistoricalTableDef.allDistrictIds(dbRunner)
//    allDistrictIds.foreach { districtId =>
//      println("Generating club info for district " + districtId)
//      ClubInfoGenerator.generateClubData(districtId, database)
//    }
    logger.info("Found " + allDistrictIds.size + " districts")

    logger.info("Fetching latest club dates")
    val latestClubDatesBase =
      HistoricClubPerfTableDef.latestDistrictMonthDates(dbRunner).toList
    latestClubDatesBase.foreach(println)
    val latestClubDates = latestClubDatesBase // .filter(v => v._2._1 != 2024 && v._2._2 != 12)

    val clubCount = latestClubDates.size
    logger.info("Found " + latestClubDates.size + " districts")

    val parallelismLevel = 4

    println("HERE2: " + latestClubDates.size)
    val futures = latestClubDates
      .grouped(clubCount / parallelismLevel)
      .map { group =>
        Future {
          group.foreach { case (districtId, (progStartYear, progStartMonth)) =>
            try {
              generateDistrictData(districtId, progStartYear, progStartMonth, dbRunner)
            } catch {
              case e: Exception =>
                logger.error(s"Failed to load district data $districtId", e)
//                System.exit(1)
            }
          }
        }
      }
      .toList

    Await.result(Future.sequence(futures), Duration.Inf)
  }
}
