package app.db

import app.db.test.{TestObj, TestTableDef}

object DBTest {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val ds = DataSource.pooled("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL")
    ds.transaction(implicit conn => {

      conn.create(TestTableDef)

      conn.insert(TestObj(1, "ABC", "DEF"), TestTableDef)
      conn.update(TestObj(1, "DEF", "AXXX"), TestTableDef)
      conn.upsert(TestObj(1, "A111BC", "D222EF"), TestTableDef)
      conn.search(TestTableDef.pkSearch("1")).foreach(println)
    })

  }
}