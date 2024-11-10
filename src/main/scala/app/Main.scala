package app

import app.data.club.info.ClubInfoGenerator
import app.data.club.perf.historical.HistoricClubPerfGenerator
import app.data.club.perf.historical.data.HistoricClubPerfTableDef
import app.data.district.historical.{HistoricDistrictPerfTableDef, HistoricalDistrictOverviewGenerator}
import app.db.DataSource

import java.text.DecimalFormat

object Main {

  private val cacheFolder = "/Users/rory.graves/Downloads/tm_cache"
  val ds                  = DataSource.pooled("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")
//  val ds = DataSource.pooled("jdbc:h2:file:/Users/rory.graves/workspace/home/testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

  // create a 2dp decimal formatter
  val df2dp: DecimalFormat = new java.text.DecimalFormat("#.##")

  def generateDistrictData(districtId: Int, dataSource: DataSource): Unit = {

    ClubInfoGenerator.generateClubData(districtId)
    HistoricClubPerfGenerator.generateHistoricalClubData(
      cacheFolder,
      districtId,
      dataSource
    )
  }

  def main(args: Array[String]): Unit = {

    ds.transaction(implicit conn => {
      conn.create(HistoricClubPerfTableDef)
      conn.create(HistoricDistrictPerfTableDef)
    })
    HistoricalDistrictOverviewGenerator.generateHistoricalOverviewData(cacheFolder, ds)
    generateDistrictData(91, ds)
    generateDistrictData(71, ds)

  }
}
