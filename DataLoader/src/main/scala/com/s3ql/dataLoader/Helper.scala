/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.dataLoader

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ SQLContext, SparkSession}
import org.apache.spark.broadcast.Broadcast
import java.io.{ FileNotFoundException, IOException }
import scala.io.Source

object Helper {

  private var prefixes = scala.collection.mutable.Map[String, String]()
  case class Triple(s: String, p: String, o: String)

  def replaceSpacialChar(p: String): String = {
    p.replaceAll(":", "_").replaceAll("<|>", "")
  }

  def parseDataset(line: String, prefixes: Broadcast[scala.collection.mutable.Map[String, String]]): Triple = {

    val fields = line.split("\\s+")
    var _subject = ""
    var _predicate = ""
    var _object = ""
    if (fields.length > 2) {
      _subject = getParsedField(fields(0), prefixes.value)
      _predicate = replaceSpacialChar(getParsedField(fields(1), prefixes.value))
      _object = getParsedField(fields.drop(2).mkString(" ").stripSuffix(".").trim, prefixes.value)
    }
    return Triple(_subject, _predicate, _object)
  }

  def replaceNamespace(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {
    var _field = ""

    if (isURI(field)) {
      prefixes foreach {
        case (key, value) =>
          if (field.contains(key)) {
            _field = field.replace(key, prefixes(key))
            return _field
          }
      }
      _field = field
      return _field
    } else {
      _field = field
      return _field
    }

  }

  def isURI(field: String): Boolean = {
    if (field.contains("<") && field.endsWith(">")) {
      return true
    } else {
      return false
    }
  }

  def getParsedField(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {

    var _str = Helper.replaceNamespace(field, prefixes)
    if (_str.endsWith("/")) {
      _str = _str.substring(0, _str.length() - 1).trim()
    }
    _str
  }

  def broadcastPrefixes(spark: SparkSession): Broadcast[scala.collection.mutable.Map[String, String]] = {
    try {
      for (line <- Source.fromFile(Settings.prefixFile).getLines) {
        val arr = line.split("&&")
        prefixes += (arr(1).trim() -> arr(0).trim())
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that prefix file.")
      case e: IOException           => println("Got an IOException!")
    }

    val _prefixes = spark.sparkContext.broadcast(prefixes)
    _prefixes
  }

  def removeDirInHDFS(spark: SparkSession, dirPath: String) = {

    val hconf = spark.sparkContext.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (dfs.exists(path)) {
      dfs.delete(path, true)
    }
  }

  def createDirInHDFS(spark: SparkSession, dirPath: String) = {

    val hconf = spark.sparkContext.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (!dfs.exists(path)) {
      dfs.mkdirs(path)
    }
  }

}


