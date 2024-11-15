package app.db

import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException

import java.sql

case class Connection(underlying: sql.Connection) {
  self =>

  def create[T](tableDef: TableDef[T]): Unit = {
    var stat: sql.PreparedStatement = null
    try {
      val createStmt = tableDef.createTableStatement
      println("Creating table: " + createStmt)

      stat = underlying.prepareStatement(createStmt, sql.Statement.RETURN_GENERATED_KEYS)
      stat.executeUpdate()

      tableDef.indexes.foreach { index =>
        stat = underlying.prepareStatement(index.createStmt)
        stat.executeUpdate()
      }

    } finally {
      if (stat != null) stat.close()
    }

  }

  def search[T](search: Search[T], limit: Option[Int] = None): List[T] = {

    val keys = search.searchItems

    val whereClause =
      if (keys.nonEmpty) "WHERE " + keys.map(c => s"\"${c.key}\" = ?").mkString(" AND ")
      else ""
    val limitClause = limit.map(l => s" LIMIT $l").getOrElse("")

    val statementText = s"""SELECT * FROM "${search.tableName}" $whereClause $limitClause"""

    val preparedStatement = underlying.prepareStatement(statementText)
    keys.zipWithIndex.foreach { case (key, idx) =>
      key.valueSetter(preparedStatement, idx + 1)
    }

    val resultSet = preparedStatement.executeQuery()
    val results   = scala.collection.mutable.ListBuffer.empty[T]
    while (resultSet.next()) {
      results += search.reader(resultSet)
    }
    results.toList
  }

  def insert[T](obj: T, tableDef: TableDef[T]): Int = {
    val columnNames   = tableDef.columns.map(v => s"\"${v.name}\"").mkString(",")
    val valueHolders  = tableDef.columns.map(_ => "?").mkString(",")
    val statementText = s"""INSERT INTO "${tableDef.tableName}" ($columnNames) VALUES ($valueHolders)"""

    val preparedStatement = underlying.prepareStatement(statementText, sql.Statement.RETURN_GENERATED_KEYS)
    tableDef.columns.zipWithIndex.foreach { case (column, idx) =>
      column.setColumn(obj, preparedStatement, idx + 1)
    }
    preparedStatement.executeUpdate()

  }

  def update[T](obj: T, tableDef: TableDef[T]): Int = {
    val valueColumns = tableDef.columns.filter(!_.primaryKey)
    val keyColumns   = tableDef.columns.filter(_.primaryKey)

    val setValues    = valueColumns.map(c => s"\"${c.name}\" = ?").mkString(" ,")
    val whereColumns = keyColumns.map(c => s"\"${c.name}\" = ?").mkString(" ,")

    val statementText = s"""UPDATE "${tableDef.tableName}" SET $setValues WHERE $whereColumns"""

    val preparedStatement = underlying.prepareStatement(statementText, sql.Statement.RETURN_GENERATED_KEYS)

    val noValueColumns = valueColumns.size

    // set the set values
    valueColumns.zipWithIndex.foreach { case (column, idx) =>
      column.setColumn(obj, preparedStatement, idx + 1)
    }

    // set the where clause values
    keyColumns.zipWithIndex.foreach { case (column, idx) =>
      column.setColumn(obj, preparedStatement, idx + 1 + noValueColumns)
    }
    preparedStatement.executeUpdate()
  }

  def upsert[T](obj: T, tableDef: TableDef[T]): Int = {
    try {
      insert(obj, tableDef)
    } catch {
      case _: JdbcSQLIntegrityConstraintViolationException =>
        // row with PK already exists - update instead
        update(obj, tableDef)
    }
  }
}
