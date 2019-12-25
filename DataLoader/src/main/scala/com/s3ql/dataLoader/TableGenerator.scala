/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.dataLoader

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.apache.spark.sql.functions._
import java.io.{ FileNotFoundException, IOException }
import scala.io.Source

object TableGenerator {

  private val spark = Settings.sparkSession
  private val _dataset = Settings.inputDataset
  private val storage = Settings.schema
  import spark.implicits._

  def generateDataSet() = {
    if (storage == "PT") {
      generatePT()
    } else if (storage == "PTP") {
      createPTP("ptp")
      //      Helper.removeDirInHDFS(spark, Settings.ptDir)
    } else {
      println("Schema not found!")
    }
  }

  def generatePT() = {
    println("Creating Property Table......")
    Helper.removeDirInHDFS(spark, Settings.ptDir)
    Helper.createDirInHDFS(spark, Settings.ptDir)

    val prefixes = Helper.broadcastPrefixes(spark)
    val rdfDF = spark.read.text(_dataset).map(r => Helper.parseDataset(r.mkString, prefixes))
    val arrayCols = rdfDF.groupBy("s", "p").agg(count("*").as("rowCount"))
      .where($"rowCount" > 1).select("p").distinct.collect.map(row => row.getString(0))
    val aggDF = rdfDF.groupBy("s").pivot("p").agg(collect_list("o"))
    val stringCols = aggDF.columns.filter(x => x != "s" && !arrayCols.contains(x))
    val ptDF = stringCols.foldLeft(aggDF)((df, x) => df.withColumn(x, col(x)(0)))
    val finalPT = arrayCols.foldLeft(ptDF)((df, x) => df.withColumn(x, when(size(col(x)) > 0, col(x)).otherwise(lit(null))))
    finalPT.write.mode(SaveMode.Overwrite).parquet(Settings.ptDir)
    println("Done!")
  }

  private def createPTP(schema: String) = {
    println("Creating PTP Tables......")
    Helper.removeDirInHDFS(spark, Settings.ptpDir)
    Helper.createDirInHDFS(spark, Settings.ptpDir)
    StatisticWriter.initNewStatisticFile(schema)
    
    var pt: DataFrame = null
    var props = Array.empty[String]
    try {
      pt = spark.read.parquet(Settings.ptDir)
      props = pt.columns.filter(x => x != "s")
    } catch {
      case e: Exception => println("Cannot find property table directory! \n"
        + "May be property table is not created, please create property table!!!" + "\n" + e)
    }

    var heavy_load = false
    if (Settings.light_load == "NO") {
      heavy_load = true
    }

    var tabName = null: String
    var tabNameInit = "$$"
    var rCount = 0: Long
    var tabCount = 0: Int
    var ptpDF: DataFrame = null
    for (i <- 0 to (props.length - 1)) {
      var atr = props(i)
      if (atr != "") {
        ptpDF = pt.filter(pt(atr).isNotNull)
        tabName = atr
        if (heavy_load) {
          props.foreach(c => {
            if (ptpDF.filter(ptpDF(c).isNotNull).count() == 0)
              ptpDF = ptpDF.drop(c)
          })
        }
        rCount = ptpDF.count()
        StatisticWriter.addTableStatistic(tabNameInit, tabName, rCount)
        StatisticWriter.incSavedTables()
        ptpDF.write.mode(SaveMode.Overwrite).parquet(Settings.ptpDir + tabName + ".parquet")
        tabCount = tabCount + 1
      }
    }

    var multivaluedFields = List[String]()
    val str = pt.schema.mkString("&").replace("StructField(", "").replace(",true)", "").replace("StringType", "String").replace("ArrayType", "Array").replace("(String", "")
    val arr = str.split("&")
    arr.foreach(e => {
      val se = e.split(",")
      if (se(1) == "Array") {
        multivaluedFields ::= se(0)
      }
    })

    StatisticWriter.closeStatisticFile(multivaluedFields.mkString("\t"), "")
    println("Done!")
  }

}

