package com.github.rorygraves.tm_data.db

import com.github.rorygraves.tm_data.util.DBRunner
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.StringWriter

trait AbstractTable[T] {

  def tableName: String
  val columns: List[Column[T]]
  def dbRunner: DBRunner

  def exportToCSV(data: Seq[T]): String = {
    import scala.jdk.CollectionConverters.IterableHasAsJava

    val out     = new StringWriter()
    val printer = new CSVPrinter(out, CSVFormat.RFC4180)

    // output the headers
    printer.printRecord(columns.map(_.name).asJava)
    // output the rows
    data.foreach { point =>
      val rowValues = columns.map(c => c.csvExportFn(point))
      printer.printRecord(rowValues.asJava)
    }

    out.toString
  }

  protected def createTableFromStatements(statements: Iterator[String]) = {

    var createdTable = false
    statements.foreach { stmt =>
      val updated = {
        if (stmt.startsWith("create index "))
          stmt.replace("create index ", "create index if not exists ")
        else if (stmt.startsWith("create unique index "))
          stmt.replace("create unique index ", "create unique index if not exists ")
        else
          stmt
      }

      import slick.jdbc.PostgresProfile.api._
      if (stmt.contains(" add constraint ") && !createdTable) {
        println("Skipping constraint creation because table does not exist")
      } else {
        println("-----------------------------------------------------------")
        println("Running statement: " + updated)
        val res: Int = dbRunner.dbAwait(sqlu"""#$updated""")
        if (res != 0 && stmt.startsWith("create table")) {
          createdTable = false
        }
      }
    }
  }

}
