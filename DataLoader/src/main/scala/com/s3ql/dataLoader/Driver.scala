/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.dataLoader

import org.apache.log4j._

object Driver {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var rdf_file: String = ""
    var prefix_file: String = ""
    var output_dir: String = ""
    var dataset: String = ""
    var scale: String = ""
    var schema: String = ""
    var load = ""
    var curArg = 0

    while (curArg < args.length) {
      args(curArg) match {
        case "--rdf-file" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF dataset in N-Triples format given!")
          rdf_file = args(curArg + 1)
          curArg += 1
        case "--prefix-file" =>
          if (curArg + 1 == args.length)
            throw new Exception("No RDF dataset in N-Triples format given!")
          prefix_file = args(curArg + 1)
          curArg += 1
        case "--output-dir" =>
          if (curArg + 1 == args.length)
            throw new Exception("No hdfs-directory to store RDF data given!")
          output_dir = args(curArg + 1)
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
        case "--schema" =>
          if (curArg + 1 == args.length)
            throw new Exception("No storage schema given!")
          schema = args(curArg + 1).toUpperCase()
          curArg += 1
        case "--light-load" =>
          if (curArg + 1 == args.length)
            throw new Exception("No load type given!")
          load = args(curArg + 1).toUpperCase()
          curArg += 1
        case _ =>
          throw new Exception("Invalid command line argument!")
      }
      curArg += 1
    }
    
    Settings.userSettings(rdf_file, prefix_file, output_dir, dataset, scale, schema, load)
    val dataLoadST = System.currentTimeMillis()
    TableGenerator.generateDataSet()
    val dataLoadET = System.currentTimeMillis()
    println("Data load time:- " + (dataLoadET - dataLoadST) + " Millis, in Second:- " + (dataLoadET - dataLoadST) * 0.001 + " Sec")

    Settings.sparkSession.stop()

  }

}
