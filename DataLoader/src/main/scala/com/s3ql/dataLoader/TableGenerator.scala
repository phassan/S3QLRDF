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
    val arrayColumns = rdfDF.groupBy("s", "p").agg(count("*").as("rowCount"))
      .where($"rowCount" > 1).select("p").distinct.collect.map(row => row.getString(0))
    val aggDF = rdfDF.groupBy("s").pivot("p").agg(collect_list("o"))
    val stringColumns = aggDF.columns.filter(x => x != "s" && !arrayColumns.contains(x))
    val ptDF1 = stringColumns.foldLeft(aggDF)((df, x) => df.withColumn(x, col(x)(0)))
    val ptDF2 = arrayColumns.foldLeft(ptDF1)((df, x) => df.withColumn(x, when(size(col(x)) > 0, col(x)).otherwise(lit(null))))
    val cols = ptDF2.columns.map(Helper.replaceSpacialChar)
    val finalPT = ptDF2.toDF(cols: _*)
    finalPT.write.mode(SaveMode.Append).parquet(Settings.ptDir)
    println("Done!")
  }

  private def createPTP(schema: String) = {
    println("Creating PTP Tables......")
    Helper.removeDirInHDFS(spark, Settings.ptpDir)
    Helper.createDirInHDFS(spark, Settings.ptpDir)

    StatisticWriter.initNewStatisticFile(schema)

    var pt: DataFrame = null
    try {
      pt = spark.read.parquet(Settings.ptDir)
    } catch {
      case e: Exception => println("Cannot find property table directory! \n"
        + "May be property table is not created, please create property table!!!" + "\n" + e)
    }

    val fields = pt.columns
    var heavy_load = true
    if (Settings.light_load == "YES") {
      heavy_load = false
    }

    var tabName = null: String
    var tabNameInit = "$$"
    var rCount = 0: Long
    var tabCount = 0: Int
    for (i <- 1 to (fields.length - 1)) {
      var atr = fields(i)

      if (atr != "") {
        var fdf = pt.filter(pt(atr).isNotNull)
        if (!(fdf.head(1).isEmpty)) {
          tabName = atr
          if (heavy_load) {
            // keep all columns of a DataFrame which contain a non-null value
            fdf.columns.foreach(c => {
              if (fdf.filter(fdf(c).isNotNull).count() == 0)
                fdf = fdf.drop(c)
            })
            //
          }

          rCount = fdf.count()

          StatisticWriter.addTableStatistic(tabNameInit, tabName, rCount)
          StatisticWriter.incSavedTables()
          fdf.write.mode(SaveMode.Append).parquet(Settings.ptpDir + tabName + ".parquet")
          tabCount = tabCount + 1
        }
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

