/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.queryExecutor

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import java.util.Calendar
import collection.mutable.LinkedHashMap

object QueryExecutor {
  private val spark = Settings.sparkSession
  import spark.implicits._

  case class Query(qName: String, tViews: ListBuffer[String], query: String)

  private def parseQueryFile(): ListBuffer[Query] = {
    var _queries = new ListBuffer[Query]()
    val queries = scala.io.Source.fromFile(Settings._queryFile).mkString.split("#####")

    for (query <- queries if query.length > 0) {
      var parts = query.split(">>>>>")
      if (parts.length == 3) {
        var _qr = Query(parts(0).trim(), parts(1).trim().split("\t").to[ListBuffer], parts(2).trim())
        _queries += _qr
      }
    }

    _queries
  }

  def executeQuery() = {
    var _resultTimes = LinkedHashMap[String, String]()
    val queries = parseQueryFile()
    for (qr <- queries) {
      println("Executing query " + qr.qName)
      var _qr = qr.query
      loadTables(Settings._dbPath, qr.tViews)
      var rt = ""
      var start = System.currentTimeMillis()
      val q = spark.sql(_qr)
      //      q.show(2)
      var resSize = q.count()
      var time = System.currentTimeMillis - start
      rt += time
      rt = rt + "ms [" + resSize + "]"

      _resultTimes(qr.qName) = rt
      println("\t" + qr.qName + " took " + time + "ms " + "[" + resSize + "]\n")
    }

    printResults(_resultTimes)
    println("Done....!")

  }

  def executeQuery_2() = {
    var _resultTimes = LinkedHashMap[String, String]()
    val queries = parseQueryFile()
    for (qr <- queries) {
      println(qr.qName + " .... ")
      var _qr = qr.query
      loadTables_2(Settings._dbPath, qr.tViews)
      println("Executing " + qr.qName)
      var rt = ""
      var start = System.currentTimeMillis()
      val q = spark.sql(_qr)
      var resSize = q.count()
      var time = System.currentTimeMillis - start
      rt += time
      rt = rt + "ms [" + resSize + "]"

      _resultTimes(qr.qName) = rt
      println("Done executing " + qr.qName + "!")
      unloadTables(qr.tViews)
      println("\t" + qr.qName + " took " + time + "ms " + "[" + resSize + "]\n")
    }

    printResults(_resultTimes)
    println("Done....!")

  }

  def printResults(results: LinkedHashMap[String, String]) = {
    val fw = new java.io.FileWriter(Settings._resultFile, true)
    var date = "" + Calendar.getInstance().getTime()
    try {
      fw.write(date + "\n")
      for ((qn, rt) <- results) {
        fw.write(qn + " took " + rt + "\n")
      }
    } finally fw.close()

  }

  private def loadTables(_path: String, _tables: ListBuffer[String]) = {
    _tables.foreach(t => {
      var table: DataFrame = null
      table = spark.sqlContext.read.parquet(_path + t + ".parquet")
      table.createOrReplaceTempView(t)
    })
  }

  private def loadTables_2(_path: String, _tables: ListBuffer[String]) = {
    _tables.foreach(t => {
      println("\tLoad Table " + t + " -> ")
      var table: DataFrame = null
      table = spark.sqlContext.read.parquet(_path + t + ".parquet")
      table.createOrReplaceTempView(t)
      spark.sqlContext.cacheTable(t)

      var start = System.currentTimeMillis
      var size = table.count()
      var time = System.currentTimeMillis - start
      println("\t\tCached " + size + " Elements in " + time + "ms")
    })
  }

  private def unloadTables(_tables: ListBuffer[String]) {
    _tables.foreach(t => {
      print("\t\tUncache Table " + t + "->")
      var start = System.currentTimeMillis
      spark.sqlContext.dropTempTable(t)
      var time = System.currentTimeMillis - start
      println(" Uncached  in " + time + "ms")
    })
    spark.sqlContext.clearCache()
  }

}