package app.db

case class IndexDef[T](idxName: String, tableDef: TableDef[T], columns: List[String], unique: Boolean) {
  def createStmt: String = {

    val uniqueStr = if (unique) "UNIQUE " else ""
    val columnIds = columns.map(c => s"\"$c\"").mkString(", ")
    s"""CREATE $uniqueStr INDEX IF NOT EXISTS "${tableDef.tableName}$idxName\"
       |ON \"${tableDef.tableName}\" ($columnIds);""".stripMargin

  }

}
