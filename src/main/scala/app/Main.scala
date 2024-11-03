package app

import app.club.info.ClubInfoGenerator
import app.club.perf.historical.HistoricClubPerfGenerator
import app.club.perf.historical.data.TMClubDataPoint

object Main {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  // create a 2dp decimal formatter
  val df2dp = new java.text.DecimalFormat("#.##")

  def main(args: Array[String]): Unit = {

    def generateDistrictData(districtId: Int): Unit = {
//      ClubInfoGenerator.generateClubData(districtId)
      HistoricClubPerfGenerator.generateHistoricalClubData(
        cacheFolder,
        districtId
      )
    }

    generateDistrictData(91)
//    generateDistrictData(71)
  }

}
