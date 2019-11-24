/* Copyright Mahmudul Hassan
 * School of Computing, Informatics, and Decision Systems Engineering
 * Arizona State University
 */

package com.s3ql.dataLoader

import java.io._

object StatisticWriter {

  private var _predicatesNum = 0: Int
  private var _statisticFileName = "": String
  private var _tripleStatFile = "": String
  private var _savedTables = 0: Int

  def initNewStatisticFile(schema: String) = {
    _statisticFileName = ("stat_" + Settings.benchmark + "_" + Settings.scale + "_" + schema.toLowerCase() + ".txt")
    _savedTables = 0

    val fw = new FileWriter(_statisticFileName, false)
    try {
      fw.write("\t" + " Statistic of " + Settings.benchmark + "_" + Settings.scale + "_"  + schema.toLowerCase() + "\n")
      fw.write("---------------------------------------------------------\n")
    } finally fw.close()
  }

  def addTableStatistic(properties: String, tableName: String, tableSize: Long) = {
    var line = properties + "	" + tableName
    line += ("\t" + tableSize)
    val fw = new FileWriter(_statisticFileName, true)
    try {
      fw.write(line + "\n")
    } finally fw.close()
  }

  def closeStatisticFile(mva: String, comn: String) = {
    val fw = new FileWriter(_statisticFileName, true)
    try {
      fw.write("---------------------------------------------------------\n")
      fw.write("###" + "\t" + mva + "\n")
      fw.write("Saved tables" + "\t" + _savedTables + "\n")
    } finally fw.close()
  }

  def incSavedTables() = {
    _savedTables += 1
  }

}