package app.data

import java.sql.{PreparedStatement, ResultSet}

abstract class Search[T] {
  def tableName: String
  def searchItems: List[SearchItem]
  def reader: ResultSet => T
}

case class SearchItem(key: String, valueSetter: (PreparedStatement, Int) => Unit)
