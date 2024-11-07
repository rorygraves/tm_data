package app

import app.club.info.ClubInfoGenerator
import app.club.perf.historical.{HistoricClubPerfGenerator, TMClubDataPoint}
import app.district.overview.DistrictOverviewGenerator

object Main {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  // create a 2dp decimal formatter
  val decimalFormatter = new java.text.DecimalFormat("#.##")

  def main(args: Array[String]): Unit = {

    def generateDistrictData(districtId: Int): Unit = {
      ClubInfoGenerator.generateClubData(districtId)
      HistoricClubPerfGenerator.generateHistoricalClubData(
        cacheFolder,
        districtId
      )
      DistrictOverviewGenerator.generateDistrictOverview(districtId, "2023-2024")
    }

    generateDistrictData(91)
    generateDistrictData(71)
  }

}
