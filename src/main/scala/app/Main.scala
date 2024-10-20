package app

import app.club.{ClubInfoGenerator, HistoricClubPerfGenerator, TMClubDataPoint}

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
    }

    generateDistrictData(91)
    generateDistrictData(71)
  }

}
