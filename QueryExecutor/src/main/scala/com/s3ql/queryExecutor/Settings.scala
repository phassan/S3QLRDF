/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.queryExecutor

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession 

object Settings {

  val sparkConf = getSparkConf()
  val sparkSession = getSparkSession()

  var _databaseDir = ""
  var _dbName = ""
  var _dbPath = ""
  var _queryFile = ""
  var _resultFile = ""

  def userSettings(dbDirPath: String,
                   dbName: String, queryFile: String) = {

    this._databaseDir = dbDirPath
    if (dbDirPath.charAt(dbDirPath.length - 1) != '/')
      this._databaseDir += "/"

    this._dbName = dbName;
    this._dbPath = _databaseDir + dbName
    if (_dbPath.charAt(_dbPath.length - 1) != '/')
      this._dbPath += "/"

    this._dbPath = _dbPath + "ptp" + "/"
    this._queryFile = queryFile
    var p = queryFile.lastIndexOf("/")
    if (p < 0) {
      this._resultFile = "./results.txt";
    } else {
      this._resultFile = queryFile.substring(0, p) + "/results.txt";
    }

  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf().setAppName("Query-Executor")
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