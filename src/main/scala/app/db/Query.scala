package app.db

import java.sql.PreparedStatement
import java.time.Instant
import java.{sql => jsql}
import scala.annotation.tailrec

/** A thin wrapper around an SQL statement */
case class Query(conn: Connection, sql: String, fillStatement: jsql.PreparedStatement => Unit) {

  def read[A](implicit r: Reader[A]): List[A] = {
    val elems                        = collection.mutable.ListBuffer.empty[A]
    var stat: jsql.PreparedStatement = null
    var res: jsql.ResultSet          = null
    try {
      stat = conn.underlying.prepareStatement(sql)
      fillStatement(stat)
      res = stat.executeQuery()

      while (res.next()) {
        elems += r.read(res)
      }
    } finally {
      if (res != null) res.close()
      if (stat != null) stat.close()
    }
    elems.result()
  }

  def readOne[A](implicit r: Reader[A]): A = read[A].head

  def readOpt[A](implicit r: Reader[A]): Option[A] = read[A].headOption

  def write(): Int = {
    var stat: jsql.PreparedStatement = null
    try {
      stat = conn.underlying.prepareStatement(sql, jsql.Statement.RETURN_GENERATED_KEYS)
      fillStatement(stat)
      stat.executeUpdate()
    } finally {
      if (stat != null) stat.close()
    }
  }
}

object Query {

  def sqlImpl(c: Connection, sc0: StringContext, args0: Seq[Any]): Query = {
    val args = args0

    @tailrec
    def write(stat: PreparedStatement, value: Any, index: Int): Unit = {
      value match {
        case v: Byte           => stat.setByte(index, v)
        case v: Short          => stat.setShort(index, v)
        case v: Int            => stat.setInt(index, v)
        case v: Long           => stat.setLong(index, v)
        case v: Float          => stat.setFloat(index, v)
        case v: Double         => stat.setDouble(index, v)
        case v: Boolean        => stat.setBoolean(index, v)
        case v: String         => stat.setString(index, v)
        case v: Array[Byte]    => stat.setBytes(index, v)
        case v: BigDecimal     => stat.setBigDecimal(index, v.bigDecimal)
        case v: java.util.UUID => stat.setObject(index, v)
        case v: Instant        => stat.setLong(index, v.getEpochSecond)
        case v: Option[_] =>
          v match {
            case Some(value) => write(stat, value, index)
            case None        => stat.setNull(index, jsql.Types.NULL)
          }
        // Add other cases for each SimpleWriter type
        case _ => throw new IllegalArgumentException("No writer found")

      }
    }

    val qstring = sc0.parts.mkString(" ? ")

    Query(
      c,
      qstring,
      (stat: jsql.PreparedStatement) => {
        for ((arg, idx) <- args.zipWithIndex) {
          write(stat, arg, idx + 1)
        }
      }
    )
  }
}
