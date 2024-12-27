package com.github.rorygraves.tm_data.db

import java.text.DecimalFormat
import java.time.LocalDate

abstract class Column[T](val name: String) {
  type I
  def exportFn: T => I
  def csvExportFn(v: T): String = exportFn(v).toString
}

case class StringColumn[T](
    override val name: String,
    override val exportFn: T => String
) extends Column[T](name) {
  override type I = String

}

case class OptionalStringColumn[T](
    override val name: String,
    override val exportFn: T => Option[String]
) extends Column[T](name) {
  override type I = Option[String]
}

case class IntColumn[T](override val name: String, override val exportFn: T => Int) extends Column[T](name) {
  override type I = Int
}

case class DoubleColumn[T](
    override val name: String,
    override val exportFn: T => Double,
    formatter: DecimalFormat
) extends Column[T](name) {
  override type I = Double
  override def csvExportFn(v: T): String = formatter.format(exportFn(v))
}

case class BooleanColumn[T](
    override val name: String,
    override val exportFn: T => Boolean
) extends Column[T](name) {
  override type I = Boolean

}

case class LocalDateColumn[T](
    override val name: String,
    override val exportFn: T => LocalDate
) extends Column[T](name) {
  override type I = LocalDate
}

case class OptionalLocalDateColumn[T](
    override val name: String,
    override val exportFn: T => Option[LocalDate]
) extends Column[T](name) {
  override type I = Option[LocalDate]

}
