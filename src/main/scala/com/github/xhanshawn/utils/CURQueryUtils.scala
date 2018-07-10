package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.CUR
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

trait CURQueryUtils extends LoggerHelper {
  val RICoreColumns = List(
    "reservation/ReservationARN",
    "reservation/ModificationStatus",
    "reservation/TotalReservedUnits",
    "reservation/UnitsPerReservation",
    "reservation/NumberOfReservations",
    "reservation/NormalizedUnitsPerReservation",
    "reservation/StartTime",
    "reservation/EndTime"
  )
  val RICostColumns = List(
    "reservation/UpfrontValue",
    "reservation/EffectiveCost",
    "reservation/RecurringFeeForUsage"
  )
  val UnusedRIColumns = List(
    "reservation/UnusedQuantity",
    "reservation/UnusedRecurringFee",
    "reservation/UnusedNormalizedUnitQuantity",
    "reservation/UnusedAmortizedUpfrontFeeForBillingPeriod"
  )
  val AmortizationColumns = List(
    "reservation/AmortizedUpfrontCostForUsage",
    "reservation/AmortizedUpfrontFeeForBillingPeriod",
    "reservation/UnusedAmortizedUpfrontFeeForBillingPeriod",
    "reservation/UpfrontValue"
  )

  val RIColumns = List(
    RICoreColumns,
    RICostColumns,
    UnusedRIColumns,
    AmortizationColumns).flatten.distinct

  def curRows: DataFrame
  def where(condition: String): CUR
  def select(cols: String*): CUR

  def printRows(rows: Seq[Row], df: DataFrame): Unit = {
    val cols = df.columns


    if (cols.length <= 10) {
      /* print horizontally */
      val rowSeq = rowsToSeq(rows)
      val rowsWithHeaders = List(cols.toList) ++ rowSeq
      val formattedRows = formatRows(rowsWithHeaders)
      formattedRows.map(_.mkString(", ")).foreach(println)
    } else {
      /* print vertically */
      val rowSeq = rotate(rowsToSeq(rows))
      val rowsWithHeaders = cols.map(Seq(_)).zip(rowSeq).map {
        case (header, row) => header ++ row
        case _ => null
      }
      rowsWithHeaders.map(_.mkString(",")).foreach(println)
    }
  }

  def printRows(rows: Seq[Row]): Unit = {
    printRows(rows, curRows)
  }

  def printRows(df: DataFrame = curRows): Unit = {
    log.warn("Printing out rows from DataFrame query. It can take a long time.")
    printRows(df.collect(), df)
  }

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

  def formatRows(rows: Seq[Seq[String]]): Seq[Seq[String]] = {
    val fomattedRotatedRows = rotate(rows).map { row =>
      val maxLength = row.map(_.length).reduceLeft(_ max _)
      row.map(_.padTo(maxLength, " ").mkString)
    }
    rotate(fomattedRotatedRows)
  }

  def rotate(rows: Seq[Seq[String]]): Seq[Seq[String]] = {
    rows.head.indices.map(i => rows.map(_(i)))
  }

  def ec2 = where("`lineItem/ProductCode` = 'AmazonEC2'")
  def ri = where("`reservation/ReservationARN` != null")
  def summary = where("`lineItem/LineItemType` = 'RIFee'")
  def riCols = select(RIColumns: _*)
  def riCostCols: CUR = {
    val cols = List(RICoreColumns, RICostColumns).flatten.distinct
    select(cols: _*)
  }

  val TempDFFileName = "tmp-df-file"
  def write(partitionNum: Int = 1): DataFrameWriter[Row] = {
    if (partitionNum == 1) {
      val tempFile = HDFSUtils.getTempFilePath(TempDFFileName).toString
      HDFSUtils.clearTempFile(tempFile)
      curRows.write.parquet(tempFile)
      val spark = sparkSessionBuilder.build()
      val df = spark.read.parquet(tempFile)
      df.repartition(partitionNum).write
    } else {
      curRows.repartition(partitionNum).write
    }
  }

  def write: DataFrameWriter[Row] = write(1)
}
