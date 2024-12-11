package com.github.rorygraves.tm_data.util

import org.slf4j.{Logger, LoggerFactory}
import slick.basic.DatabaseConfig
import slick.dbio.{DBIOAction, NoStream}
import slick.jdbc.JdbcProfile
import slick.jdbc.PostgresProfile.api._

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

abstract class DBRunner {

  def db: Database;
  protected def logger: Logger = LoggerFactory.getLogger(getClass)
  private val actionTimeout    = Duration(20, TimeUnit.SECONDS)

  def dbAwait[T](a: DBIOAction[T, NoStream, Nothing], waitingFor: String = "", atMost: Duration = actionTimeout): T = {
    val startTime = System.currentTimeMillis()
    val dbExec    = db.run(a)
    dbExec.onComplete {
      case _: Success[T] =>
      // do nothing - result consumed below
      case Failure(exception) =>
        val endTime  = System.currentTimeMillis()
        val duration = endTime - startTime
        logger.error(s"DB Error seen during dbAwait - $waitingFor - after ${duration}ms", exception)
    }(scala.concurrent.ExecutionContext.global)

    try {
      Await.result(dbExec, atMost)
    } catch {
      case t: Throwable =>
        val ex = new IllegalStateException("Exception during await", t)
        logger.error("Got exception during action", ex)
        throw ex
    }
  }

}

class FixedDBRunner(providedDB: Database) extends DBRunner {
  override def db: Database = providedDB
}

class ConfigDBRunner(config: DatabaseConfig[JdbcProfile]) extends DBRunner {
  override def db: Database = config.db.asInstanceOf[Database]
}
