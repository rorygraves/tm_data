package com.github.rorygraves.tm_data

import app.data.club.info.ClubInfoGenerator
import app.data.club.perf.historical.HistoricClubPerfGenerator
import app.data.club.perf.historical.data.HistoricClubPerfTableDef
import app.db.DataSource
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, DistrictSummaryHistoricalTableDef}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object DataCheckerMain {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"
//  val ds                  = DataSource.pooled("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

  val ds = Sources.pooledAWS

  def main(args: Array[String]): Unit = {

    val overviewData = DistrictSummaryHistoricalTableDef.allDistrictYearMonths(ds)

    val groupedData = overviewData.groupBy(_._1)
    val distSummarySortedDataMap = groupedData.view.mapValues { data =>
      data.sortBy(_._4)
    }.toMap

    val distStartDates = DistrictSummaryHistoricalTableDef.districtStartDates(ds).toList.sortBy(v => f"$v%6s")
    distStartDates.foreach(println)

//    val distSummarySortedDataList = distSummarySortedDataMap.toList.sortBy(_._1)
//    distSummarySortedDataList.foreach { case (districtId, data) =>
//      val first = data.head
//      val last  = data.last
//
//      println(f"$districtId%6s: ${first._2},${first._3}%2d-${first._4} - ${last._2},${last._3}%2d-${last._4}")
//    }
//
//    val clubData     = HistoricClubPerfTableDef.allDistrictYearMonths(ds)
//    val groupedClubs = clubData.groupBy(_._1)
//    groupedClubs.toList.sortBy(_._1).foreach { case (districtId, data) =>
//      val sortedDates = data.map(_._4).sorted
//
//      println(f"$districtId%6s: ${sortedDates.head} - ${sortedDates.last}")
//
//    }
  }
}
