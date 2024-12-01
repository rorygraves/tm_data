package com.github.rorygraves.tm_data.db

import java.sql.{PreparedStatement, ResultSet}

abstract class Search[T] {
  def tableName: String
  def searchItems: List[SearchItem]
  def columns: Option[List[String]] = None
  def reader: ResultSet => T
}

case class SearchItem(key: String, valueSetter: (PreparedStatement, Int) => Unit)
