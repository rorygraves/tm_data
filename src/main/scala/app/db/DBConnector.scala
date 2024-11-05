package app.db

import app.data.{Search, TableDef}
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException

import java.{sql => jsql}
import scala.annotation.implicitNotFound

@implicitNotFound("No database connection found. Make sure to call this in a `run()` or `transaction()` block.")
case class Connection(underlying: jsql.Connection) {
  self =>
  implicit class SqlInterpolator(sc: StringContext) {
    def sql(args: Any*): Query = Query.sqlImpl(self, sc, args)
  }

  def create[T](tableDef: TableDef[T]): Unit = {
    var stat: jsql.PreparedStatement = null
    try {
      val createStmt = tableDef.createStatement
      stat = underlying.prepareStatement(createStmt, jsql.Statement.RETURN_GENERATED_KEYS)
      stat.executeUpdate()
    } finally {
      if (stat != null) stat.close()
    }

  }

  def search[T](search: Search[T], limit: Option[Int] = None): List[T] = {

    val keys = search.searchItems

    val whereClause   = keys.map(c => s"\"${c.key}\" = ?").mkString(" AND ")
    val limitClause   = limit.map(l => s" LIMIT $l").getOrElse("")
    val statementText = s"""SELECT * FROM "${search.tableName}" WHERE $whereClause $limitClause"""

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

    val preparedStatement = underlying.prepareStatement(statementText, jsql.Statement.RETURN_GENERATED_KEYS)
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

    val preparedStatement = underlying.prepareStatement(statementText, jsql.Statement.RETURN_GENERATED_KEYS)

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

trait Reader[A] {
  def read(results: jsql.ResultSet): A
}
