package com.github.rorygraves.tm_data.db.test

import com.github.rorygraves.tm_data.db._

import java.sql.ResultSet

object TestTableDef extends TableDef[TestObj] {

  val tableName = "TestTable"
  val columns: List[ColumnDef[TestObj]] = List[ColumnDef[TestObj]](
    IntColumnDef("Key", t => t.key, primaryKey = true),
    StringColumnDef("Value1", t => t.value1),
    StringColumnDef("Value2", t => t.value2)
  )

  def pkSearch(key: String): Search[TestObj] = PkSearchKey(key)

  private case class PkSearchKey(key: String) extends Search[TestObj] {
    override def tableName: String             = TestTableDef.tableName
    override def searchItems: List[SearchItem] = List(SearchItem("Key", (stmt, idx) => stmt.setString(idx, key)))
    override def reader: ResultSet => TestObj = rs =>
      TestObj(rs.getInt("Key"), rs.getString("Value1"), rs.getString("Value2"))
  }

}
