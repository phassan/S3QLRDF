/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.queryExecutor

import org.apache.log4j._

object Driver {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var output_dir: String = ""
    var dataset: String = ""
    var scale: String = ""
    var query_file: String = ""
    var cachedTable: String = ""
    var curArg = 0

    while (curArg < args.length) {
      args(curArg) match {
        case "--output-dir" =>
          if (curArg + 1 == args.length)
            throw new Exception("No S3QL storage directory given!")
          output_dir = args(curArg + 1)
          if (output_dir.charAt(output_dir.length - 1) != '/')
            output_dir += "/"
          curArg += 1
        case "--dataset" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF dataset name given!")
          dataset = args(curArg + 1)
          curArg += 1
        case "--scale" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF dataset scale factor given!")
          scale = args(curArg + 1)
          curArg += 1
        case "--cache-table" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF dataset scale factor given!")
          cachedTable = args(curArg + 1).toLowerCase
          curArg += 1
        case "--query-file" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF file given!")
          query_file = args(curArg + 1)
          curArg += 1
        case _ =>
          throw new Exception("Invalid command line argument!")
      }
      curArg += 1
    }

    var db_Path = output_dir + dataset + "/"
    Settings.userSettings(db_Path, scale, query_file)
    if (cachedTable == "yes") {
      QueryExecutor.executeQuery_2()
    } else {
      QueryExecutor.executeQuery()
    }

    Settings.sparkSession.stop()

  }

}