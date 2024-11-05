package app.db

class DataSource(getConnection: () => Connection) {

  def transaction[A](fn: Connection => A): A = {
    val conn       = getConnection()
    val underlying = conn.underlying
    try {
      underlying.setAutoCommit(false)
      val r = fn(conn)
      underlying.commit()
      r
    } catch {
      case ex: Throwable =>
        underlying.rollback()
        throw ex
    } finally {
      underlying.close()
    }
  }

  def run[A](fn: Connection => A): A = {
    val conn       = getConnection()
    val underlying = conn.underlying
    try {
      underlying.setAutoCommit(true)
      fn(conn)
    } finally {
      underlying.close()
    }
  }
}

object DataSource {
  def pooled(jdbcUrl: String, username: String = null, password: String = null): DataSource = {
    val ds = new com.zaxxer.hikari.HikariDataSource()
    ds.setJdbcUrl(jdbcUrl)
    if (username != null) ds.setUsername(username)
    if (password != null) ds.setPassword(password)
    new DataSource(() => Connection(ds.getConnection()))
  }
}
