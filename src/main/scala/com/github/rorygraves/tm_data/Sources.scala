package com.github.rorygraves.tm_data

import com.github.rorygraves.tm_data.db.DataSource

import scala.annotation.unused

// database sources for various uses
object Sources {

  private lazy val awsDbUrl  = sys.env.getOrElse("AWS_DB_URL", throw new RuntimeException("AWS_DB_URL not set"))
  private lazy val awsDbUser = sys.env.getOrElse("AWS_DB_USER", throw new RuntimeException("AWS_DB_USER not set"))
  private val awsDbPassword =
    sys.env.getOrElse("AWS_DB_PASSWORD", throw new RuntimeException("AWS_DB_PASSWORD not set"))

  @unused
  def pooledAWS: DataSource = DataSource.pooled(awsDbUrl, awsDbUser, awsDbPassword)

  @unused
  def singleAWS: DataSource = DataSource.single(awsDbUrl, awsDbUser, awsDbPassword)

  @unused
  def singleH2Mem: DataSource = DataSource.single("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

  @unused
  def pooledH2Mem: DataSource = DataSource.pooled("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")

  @unused
  def singleLocal: DataSource =
    DataSource.single(
      "jdbc:postgresql://localhost:5432/rory.graves",
      "rory.graves",
      ""
    )
}
