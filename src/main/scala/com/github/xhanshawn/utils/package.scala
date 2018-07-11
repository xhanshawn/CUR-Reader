package com.github.xhanshawn

import java.io.File

import org.apache.spark.sql.Row

package object utils {

  def getFileName(path: String): String = {
    new File(path).getName()
  }

  def ensureS3PathToFilePath(key: String): String = {
    key.split("/").map(e => if (e == "") "_" else e).mkString("/")
  }

  /**
    * Convert a Sequence of Spark Sql Rows to 2D Arrays of String.
    * @param rows
    * @return
    */
  def rowsToSeq(rows: Seq[Row]): Seq[Seq[String]] = {
    if (rows.isEmpty) return Seq(Seq())
    for {
      row <- rows
      seq = row.toSeq.map {
        case null => "null"
        case x => x.toString
      }
    } yield seq.toList
  }

  /**
    * Format a list of horizontal Spark Sql Rows.
    * @param rows
    * @return
    */
  def formatRows(rows: Seq[Seq[String]]): Seq[Seq[String]] = {
    val fomattedRotatedRows = rotate(rows).map { row =>
      val maxLength = row.map(_.length).reduceLeft(_ max _)
      row.map(_.padTo(maxLength, " ").mkString)
    }
    rotate(fomattedRotatedRows)
  }

  /**
    * Transpose a nested Spark Sql Rows.
    * @param rows
    * @return
    */
  def rotate(rows: Seq[Seq[String]]): Seq[Seq[String]] = {
    rows.head.indices.map(i => rows.map(_(i)))
  }
}
