package app.db

import java.sql.{Date, PreparedStatement, ResultSet, Types}
import java.text.DecimalFormat
import java.time.LocalDate

abstract class ColumnDef[T](val name: String) {
  type I
  def databaseTypeId: Int
  def exportFn: T => I
  def primaryKey: Boolean
  def nullable: Boolean = false
  def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit
  def decode(rs: ResultSet): I
  def columnTypeStr: String
  def columnCreateStr           = s""" $name $columnTypeStr ${if (!nullable) " NOT NULL" else ""}"""
  def csvExportFn(v: T): String = exportFn(v).toString
}

case class StringColumnDef[T](
    override val name: String,
    override val exportFn: T => String,
    primaryKey: Boolean = false,
    length: Int = -1
) extends ColumnDef[T](name) {
  override type I = String

  override def columnTypeStr: String = if (length == -1) "text" else s"varchar($length)"
  override def databaseTypeId: Int   = Types.VARCHAR

  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit =
    statement.setString(columnIdx, exportFn(v))

  def decode(rs: ResultSet): String = rs.getString(name)
}

case class OptionalStringColumnDef[T](
    override val name: String,
    override val exportFn: T => Option[String],
    primaryKey: Boolean = false,
    length: Int = -1
) extends ColumnDef[T](name) {
  override type I = Option[String]

  override def columnTypeStr: String = if (length == -1) "text" else s"varchar($length)"
  override def nullable              = true;
  override def databaseTypeId: Int   = Types.VARCHAR

  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit = {
    val vOpt = exportFn(v)
    vOpt match {
      case Some(v) =>
        statement.setString(columnIdx, v)
      case None =>
        statement.setNull(columnIdx, databaseTypeId)
    }
  }
  def decode(rs: ResultSet): Option[String] = {
    Option(rs.getString(name))
  }

}

case class IntColumnDef[T](override val name: String, override val exportFn: T => Int, primaryKey: Boolean = false)
    extends ColumnDef[T](name) {
  override type I = Int

  override def columnTypeStr: String = s"integer"
  override def databaseTypeId: Int   = Types.INTEGER
  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit =
    statement.setInt(columnIdx, exportFn(v))
  override def decode(rs: ResultSet): Int = rs.getInt(name)
}

case class DoubleColumnDef[T](
    override val name: String,
    override val exportFn: T => Double,
    formatter: DecimalFormat,
    primaryKey: Boolean = false
) extends ColumnDef[T](name) {
  override type I = Double
  override def columnTypeStr: String = "double precision"
  override def databaseTypeId: Int   = Types.DOUBLE
  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit =
    statement.setDouble(columnIdx, exportFn(v))
  override def csvExportFn(v: T): String     = formatter.format(exportFn(v))
  override def decode(rs: ResultSet): Double = rs.getDouble(name)
}

case class BooleanColumnDef[T](
    override val name: String,
    override val exportFn: T => Boolean,
    primaryKey: Boolean = false
) extends ColumnDef[T](name) {
  override type I = Boolean

  override def columnTypeStr: String = "boolean"
  override def databaseTypeId: Int   = Types.BIT

  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit =
    statement.setBoolean(columnIdx, exportFn(v))
  override def decode(rs: ResultSet): Boolean = rs.getBoolean(name)
}

case class LocalDateColumnDef[T](
    override val name: String,
    override val exportFn: T => LocalDate,
    primaryKey: Boolean = false
) extends ColumnDef[T](name) {
  override type I = LocalDate
  override def columnTypeStr: String = "date"
  override def databaseTypeId: Int   = Types.DATE
  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit =
    statement.setDate(columnIdx, Date.valueOf(exportFn(v)))

  override def decode(rs: ResultSet): LocalDate = rs.getDate(name).toLocalDate
}

case class OptionalLocalDateColumnDef[T](
    override val name: String,
    override val exportFn: T => Option[LocalDate]
) extends ColumnDef[T](name) {
  override type I = Option[LocalDate]

  override def primaryKey: Boolean   = false
  override def columnTypeStr: String = "date"
  override def nullable              = true;
  override def databaseTypeId: Int   = Types.DATE

  override def setColumn(v: T, statement: PreparedStatement, columnIdx: Int): Unit = {
    val vOpt = exportFn(v)
    vOpt match {
      case Some(v) =>
        statement.setDate(columnIdx, Date.valueOf(v))
      case None =>
        statement.setNull(columnIdx, databaseTypeId)
    }
  }

  override def decode(rs: ResultSet): Option[LocalDate] = {
    Option(rs.getDate(name)).map(_.toLocalDate)
  }
}
