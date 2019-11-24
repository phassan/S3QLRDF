/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.dataLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Settings {

  val sparkConf = getSparkConf()
  val sparkSession = getSparkSession()

  var inputDataset = ""
  var prefixFile = ""
  var benchmark = ""
  var scale = ""
  var ptpDir = ""
  var ptDir = ""
  var schema = ""
  var light_load = ""

  def userSettings(_rdfFile: String, _prefixFile: String,
                   _outputDir: String, _benchmark: String,
                   _scale: String, _schema: String,
                   _load: String) = {
    try {
      this.inputDataset = _rdfFile
      this.prefixFile = _prefixFile
      var outputDir = _outputDir
      if (_outputDir.charAt(_outputDir.length - 1) != '/')
        outputDir += "/"

      this.benchmark = _benchmark.toLowerCase()
      this.scale = _scale
      this.schema = _schema
      this.light_load = _load
      var tableDir = outputDir + benchmark + "/" + scale + "/"
      this.ptDir = tableDir + "pt.parquet"

      if (schema == "PTP") {
        this.ptpDir = tableDir + "ptp" + "/"
      }
    } catch {
      case e: Exception => println("Cannot create directory for the schema ->"
        + schema + "\n" + e)
    }
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf().setAppName("Data-Loader")
      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.parquet.filterPushdown", "true")
    //      .set("spark.storage.memoryFraction", "0.5")
    //      .set("spark.sql.shuffle.partitions", "100")
    //      .setMaster("local[*]")
    sparkConf
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    spark
  }

}