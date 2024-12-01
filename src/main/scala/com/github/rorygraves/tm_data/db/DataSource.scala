package com.github.rorygraves.tm_data.db

import java.sql.DriverManager

class DataSource(getConnection: () => Connection, close: Boolean = true) {

  def retry[A](fn: => A, maxRetries: Int): A = {
    var retryCount               = 0
    var result: Option[A]        = None
    var lastException: Throwable = null

    while (retryCount < maxRetries && result.isEmpty) {
      try {
        result = Some(fn)
      } catch {
        case ex: Throwable =>
          lastException = ex
          println(s"BANG  ${ex.getMessage}")
          retryCount += 1
      }
    }

    result.getOrElse(throw lastException)
  }

  def transaction[A](fn: Connection => A): A = {
    retry(
      {
        val conn       = getConnection()
        val underlying = conn.underlying
        val res =
          try {
            underlying.setAutoCommit(false)
            val r = fn(conn)
            underlying.commit()
            r
          } catch {
            case ex: Throwable =>
              ex.printStackTrace()
              underlying.rollback()
              throw ex
          } finally {
            if (close) {
              underlying.close()
              conn.close()
            }
          }

        res
      },
      3
    )
  }

  def run[A](fn: Connection => A): A = {
    retry(
      {
        val conn       = getConnection()
        val underlying = conn.underlying
        try {
          underlying.setAutoCommit(true)
          fn(conn)
        } finally {
          if (close) {
            underlying.close()
            conn.close()
          }
        }
      },
      3
    )
  }
}

object DataSource {
  def pooled(jdbcUrl: String, username: String = null, password: String = null): DataSource = {
    val ds = new com.zaxxer.hikari.HikariDataSource()
    ds.setJdbcUrl(jdbcUrl)
    if (username != null) ds.setUsername(username)
    if (password != null) ds.setPassword(password)
    new DataSource(() => {
      val conn = new Connection(ds.getConnection())
      conn.underlying.setNetworkTimeout(null, 20000)
      conn
    })
  }

  def single(jdbcUrl: String, username: String = null, password: String = null, close: Boolean = true): DataSource = {
    val dbConn = DriverManager.getConnection(jdbcUrl, username, password)
//    if (username != null) ds.setUsername(username)
//    if (password != null) ds.setPassword(password)

    dbConn.setNetworkTimeout(null, 20000)
    val conn = new Connection(dbConn)
    new DataSource(() => { conn }, close = close)
  }
}
