package com.github.rorygraves.tm_data


import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, ClubInfoTableDef}
import com.github.rorygraves.tm_data.data.club.perf.historical.HistoricClubPerfGenerator
import com.github.rorygraves.tm_data.data.club.perf.historical.data.HistoricClubPerfTableDef
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, DistrictSummaryHistoricalTableDef}
import com.github.rorygraves.tm_data.db.DataSource

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  def generateDistrictData(
      districtId: String,
      progStartYear: Int,
      progStartMonth: Int,
      dataSource: DataSource
  ): Unit = {

    ClubInfoGenerator.generateClubData(districtId, dataSource)

    HistoricClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      progStartYear,
      progStartMonth,
      dataSource
    )

  }

  def main(args: Array[String]): Unit = {

    val ds = Sources.pooledAWS

    ds.run(implicit conn => {
      println("Ensuring club info table exists")
      conn.create(ClubInfoTableDef)
      println("Ensuring historic club performance table exists")
      conn.create(HistoricClubPerfTableDef)
      println("Ensuring district summary historical table exists")
      conn.create(DistrictSummaryHistoricalTableDef)
    })

    println("Generating historical overview data")

    DistrictSummaryHistoricalGenerator.generateHistoricalOverviewData(cacheFolder, ds)

    val allDistrictIds = DistrictSummaryHistoricalTableDef.allDistrictIds(ds)
    allDistrictIds.foreach { districtId =>
      println("Generating club info for district " + districtId)
      ClubInfoGenerator.generateClubData(districtId, ds)
    }

    val latestClubDates =
      HistoricClubPerfTableDef.latestDistrictMonthDates(ds).toList.filter(v => v._2._1 != 2024 && v._2._2 != 11)

    latestClubDates.foreach(println)

    val parallelismLevel = 4

    val futures = latestClubDates
      .grouped(parallelismLevel)
      .map { group =>
        Future {
          group.foreach { case (districtId, (progStartYear, progStartMonth)) =>
            try {
              generateDistrictData(districtId, progStartYear, progStartMonth, ds)
            } catch {
              case e: Exception =>
                println(s"Failed to load district data $districtId")
                println(s"Exception: ${e.getMessage}")
                e.printStackTrace()
//                System.exit(1)
            }
          }
        }
      }
      .toList

    Await.result(Future.sequence(futures), Duration.Inf)
  }
}
