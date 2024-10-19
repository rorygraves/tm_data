package app

import app.club.{ClubData, TMClubDataPoint}

object Main {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"

  trait ColumnCalculator {
    def name: String
    def calculation: TMClubDataPoint => String
  }

  // create a 2dp decimal formatter
  val decimalFormatter = new java.text.DecimalFormat("#.##")

  def main(args: Array[String]): Unit = {

    ClubData.generateHistoricalClubData(cacheFolder, 91)
    ClubData.generateHistoricalClubData(cacheFolder, 71)

  }

}
