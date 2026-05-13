package com.github.rorygraves.tm_data.data.district

import com.github.rorygraves.tm_data.db._
import com.github.rorygraves.tm_data.util.DBRunner
import com.github.rorygraves.tm_data.data.district.historical.TMDataDistrictSummaryHistoricalTable
import slick.jdbc.PostgresProfile.api._
import slick.sql.SqlProfile.ColumnOption.NotNull
import slick.relational.RelationalProfile.ColumnOption.Length
import org.slf4j.LoggerFactory

case class DistrictRegion(
    district: String,
    region: String
)

class TMDataDistrictRegionTable(val dbRunner: DBRunner) extends AbstractTable[DistrictRegion] {
  private val logger     = LoggerFactory.getLogger("TMDataDistrictRegionTable")
  override val tableName = "district_region"

  class DistrictRegionTable(tag: Tag) extends Table[DistrictRegion](tag, tableName) {
    def district = column[String]("district", Length(3), NotNull)
    def region   = column[String]("region", Length(4), NotNull)

    def pk = primaryKey("pk__" + this.tableName, district)

    def * = (district, region).mapTo[DistrictRegion]
  }

  val tq = TableQuery[DistrictRegionTable]

  val columns: List[Column[DistrictRegion]] = List[Column[DistrictRegion]](
    StringColumn("district", _.district),
    StringColumn("region", _.region)
  )

  def createIfNotExists(): Unit = {
    val statements = tq.schema.createIfNotExistsStatements
    logger.debug("TMDataDistrictRegionTable.createIfNotExists")
    createTableFromStatements(statements)
  }

  def updateFromHistorical(historicalTable: TMDataDistrictSummaryHistoricalTable): Int = {
    // Get the latest data points from the historical table
    val latestData = historicalTable.getLatest()

    // Convert to DistrictRegion records
    val districtRegions = latestData.map(dp => DistrictRegion(dp.district, dp.region))

    // Delete existing data and insert new data in a transaction
    val action = DBIO
      .seq(
        districtRegions.map(tq.insertOrUpdate): _*
      )
      .transactionally

    dbRunner.dbAwait(action)
    districtRegions.size
  }

  def getRegion(district: String): Option[String] = {
    dbRunner.dbAwait(tq.filter(_.district === district).map(_.region).result.headOption)
  }

  def getAllRegions(): Map[String, String] = {
    dbRunner.dbAwait(tq.result).map(dr => dr.district -> dr.region).toMap
  }
}
