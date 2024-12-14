package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.club.info.ClubInfoGenerator
import com.github.rorygraves.tm_data.data.club.perf.historical.HistoricClubPerfGenerator
import com.github.rorygraves.tm_data.data.club.perf.historical.data.HistoricClubPerfTableDef
import com.github.rorygraves.tm_data.data.district.historical.{DistrictSummaryHistoricalGenerator, TMDataDistrictSummaryHistoricalTable}
import com.github.rorygraves.tm_data.util.FixedDBRunner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object DataCheckerMain {

  import slick.jdbc.PostgresProfile.api._

  def main(args: Array[String]): Unit = {
    val db       = Database.forConfig("tm_data")
    val dbRunner = new FixedDBRunner(db)
    try {
      val districtSummaryHistoricalTableDef = new TMDataDistrictSummaryHistoricalTable(dbRunner)
      val overviewData                      = districtSummaryHistoricalTableDef.allDistrictYearMonths()

      val groupedData = overviewData.groupBy(_._1)
      val distSummarySortedDataMap = groupedData.view.mapValues { data =>
        data.sortBy(_._4)
      }.toMap

      val distStartDates = districtSummaryHistoricalTableDef.districtStartDates().toList.sortBy(v => f"$v%6s")
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

    } finally db.close
  }
}
