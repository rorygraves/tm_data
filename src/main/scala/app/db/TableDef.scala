package app.db

trait TableDef[T] {

  def tableName: String
  val columns: List[ColumnDef[T]]

  def createTableStatement: String = {

    val columnCreateStrs = columns.map(_.columnCreateStr).mkString(",\n")
    val primaryKeys      = columns.filter(_.primaryKey)
    val primaryKeysStr =
      if (primaryKeys.nonEmpty)
        ",\n  PRIMARY KEY (" + primaryKeys.map(c => s"${c.name}").mkString(",") + ")"
      else
        ""
    s"""CREATE TABLE IF NOT EXISTS $tableName (\n$columnCreateStrs$primaryKeysStr
    );"""
  }

  def indexes: List[IndexDef[T]] = List.empty
}
