package app

import app.data.club.perf.historical.data.HistoricClubPerfTableDef
import app.data.district.historical.DistrictSummaryHistoricalTableDef

/** EndMarker */
object ExperimentationMain {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  val ds = Sources.pooledAWS

  def main(args: Array[String]): Unit = {

    ds.run(implicit conn => {
      println("Creating history table")
      conn.create(HistoricClubPerfTableDef)
      println("Creating summary table")
      conn.create(DistrictSummaryHistoricalTableDef)
    })

    println("Generating historical overview data")
  }
}
