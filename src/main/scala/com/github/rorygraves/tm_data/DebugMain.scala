package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.data.area.perf.{HistoricAreaPerfTable, TMAreaDataDownloader}
import com.github.rorygraves.tm_data.data.club.info.{ClubInfoGenerator, TMDataClubInfoTable}
import com.github.rorygraves.tm_data.data.club.perf.{HistoricClubPerfGenerator, HistoricClubPerfTable}
import com.github.rorygraves.tm_data.data.district.historical.{
  DistrictSummaryHistoricalGenerator,
  TMDataDistrictSummaryHistoricalTable
}
import com.github.rorygraves.tm_data.data.district.{
  DistrictImportResult,
  FailedDistrictImportResult,
  SuccessfulDistrictImportResult
}
import com.github.rorygraves.tm_data.data.division.{HistoricDivisionPerfTableDef, TMDivisionDataDownloader}
import com.github.rorygraves.tm_data.util.{DBRunner, FixedDBRunner}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.PostgresProfile.api._

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

object DebugMain {

  val logger: Logger      = LoggerFactory.getLogger(getClass)
  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  val totalLoads = new AtomicInteger(0)
  val totalTime  = new AtomicLong(0)
  def main(args: Array[String]): Unit = {

    val db       = Database.forConfig("tm_data")
    val dbRunner = new FixedDBRunner(db)
    try {
      val clubInfoTable                      = new TMDataClubInfoTable(dbRunner)
      val clubInfoGenerator                  = new ClubInfoGenerator(clubInfoTable)
      val districtSummaryHistoricalTable     = new TMDataDistrictSummaryHistoricalTable(dbRunner)
      val districtSummaryHistoricalGenerator = new DistrictSummaryHistoricalGenerator(districtSummaryHistoricalTable)
      val historicClubPerfTable              = new HistoricClubPerfTable(dbRunner)
      val historicClubPerfGenerator =
        new HistoricClubPerfGenerator(districtSummaryHistoricalTable, historicClubPerfTable)

      val historicAreaPerfTableDef = new HistoricAreaPerfTable(dbRunner)
      val areaDownloader           = new TMAreaDataDownloader(historicAreaPerfTableDef)

      val historicDivisionPerfTableDef = new HistoricDivisionPerfTableDef(dbRunner)
      val divisionDownloader           = new TMDivisionDataDownloader(historicDivisionPerfTableDef)

      var missing = 0
//      historicClubPerfGenerator.downloadHistoricalClubData(2012, "17", Some(12), Main.cacheFolder, single = true)

      val clubDistrictYearMonths = historicClubPerfTable.allDistrictYearMonths().groupBy(_._1).toMap

      val allDistricts = districtSummaryHistoricalTable.allDistrictIds().sorted
      allDistricts
        .foreach(districtId => {
          val expectedMonths     = districtSummaryHistoricalTable.datesForDistrict(districtId)
          val start              = expectedMonths.head
          val end                = expectedMonths.last
          val clubDistrictMonths = clubDistrictYearMonths.getOrElse(districtId, List.empty)

//          println("Club months = " + clubDistrictMonths.size)
//
          val summaryMonths = expectedMonths.map(r => (r._1, r._2) -> r._3)
          val clubMonths    = clubDistrictMonths.map(r => (r._2, r._3) -> r._4).toMap

          val diff = summaryMonths.size - clubMonths.size
//          if (diff < 10) {
          if (summaryMonths.size != clubMonths.size) {

            println(
              s"District $districtId - $start to $end - distEntries: ${summaryMonths.size}, club entries: ${clubDistrictMonths.size}  ($diff)"
            )

            var count = 0
            var inGap = false

            for (summaryMonth <- summaryMonths) {
              if (!clubMonths.contains(summaryMonth._1)) {
                missing += 1
                if (!inGap) {
                  println(s"  Missing ${summaryMonth._1}")
                  //     inGap = true
                }

                count = count + 1

                if (count < 1) {
                  val start = System.currentTimeMillis()
                  historicClubPerfGenerator.downloadHistoricalClubData(
                    summaryMonth._1._1,
                    districtId,
                    Some(summaryMonth._1._2),
                    Main.cacheFolder,
                    single = true
                  )

                  val end            = System.currentTimeMillis()
                  val time           = end - start
                  val totalLoadsTime = totalLoads.incrementAndGet()
                  val totalNow       = totalTime.addAndGet(time)
                  println(s"  Download took ${end - start}ms  - avg = ${totalNow / totalLoadsTime}ms")
                }
              } else {
                if (inGap) {
                  println(s"  Found ${summaryMonth._1}")
                  inGap = false
                }
              }
            }
          }
        })

      println(s"Missing = $missing")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)

    } finally db.close

  }
}
