package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.CUR
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

trait CURQueryUtils extends LoggerHelper {
  /**
    * Contants for common used CUR columns.
    */
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

  /**
    * Abstract methods to support DataFrame query chaining over CUR case class.
    */
  def curRows: DataFrame
  def initWithDF(df: DataFrame): CUR
  def where(condition: String): CUR = {
    log.info(s"added where clause ${condition}")
    initWithDF(curRows.where(condition))
  }
  def where(condition: Column): CUR = {
    log.info(s"added where clause ${condition}")
    initWithDF(curRows.where(condition))
  }
  def select(cols: String*): CUR = {
    log.info(s"added select clause ${cols.mkString(", ")}")
    val df = curRows.select(cols.head, cols.tail :_*)
    initWithDF(df)
  }
  def withColumn(str: String, column: Column): CUR = {
    val df = curRows.withColumn(str, column)
    initWithDF(df)
  }
  def aggAfterGroupBy(aggExp: Column, groupCols: String*): CUR = {
    val df = curRows.groupBy(groupCols.map(col): _*).agg(aggExp)
    initWithDF(df)
  }

  /**
    * Print Spark Sql rows passed in. If the column num is small, we try to print it horizontally.
    * However if there are many columns, it will print rows vertically.
    * @param rows A Sequence of Spark Sql rows
    * @param df   DataFrame that provides the column definitions.
    */
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

  /**
    * print Spark Sql rows with member variable curRows.
    * @param df
    */
  def printRows(df: DataFrame = curRows): Unit = {
    log.warn("Printing out rows from DataFrame query. It can take a long time.")
    printRows(df.collect(), df)
  }

  /**
    * Predefined query helpers including service, line item type, etc.
    */

  /**
    * Helpers to query CUR rows by services.
    */
  def prodIs(prodName: String) = where(s"`lineItem/ProductCode` = '$prodName'")
  def ec2 = prodIs("AmazonEC2")
  def rds = prodIs("AmazonRDS")

  /**
    * Helpers to query CUR rows by reservation types.
    */
  def ri = where("`reservation/ReservationARN` IS NOT null OR `reservation/ReservationARN` <> ''")
  def od = where("`reservation/ReservationARN` IS null OR `reservation/ReservationARN` = ''")
  def allUpfront = where(
    """
      |(`pricing/PurchaseOption` IS NOT NULL AND `pricing/PurchaseOption` = 'All Upfront') OR
      |(`reservation/UpfrontValue` IS NOT null AND `reservation/UpfrontValue` > 0 AND `RecurringFeeForUsage` = 0)
      |""".stripMargin)

  def partialUpfront = where(
    """
      |(`pricing/PurchaseOption` IS NOT NULL AND `pricing/PurchaseOption` = 'Partial Upfront') OR
      |(`reservation/UpfrontValue` IS NOT null AND `reservation/UpfrontValue` > 0 AND `RecurringFeeForUsage` > 0)
      |""".stripMargin)

  def noUpfront = where(
    """
      |(`pricing/PurchaseOption` IS NOT NULL AND `pricing/PurchaseOption` = 'No Upfront') OR
      |((`reservation/UpfrontValue` IS null OR `reservation/UpfrontValue` = 0) AND `RecurringFeeForUsage` > 0)
      |""".stripMargin)

  /**
    * Helpers to query CUR rows by line item type.
    */
  def rowTypeIs(rowType: String) = where(s"`lineItem/LineItemType` = '$rowType'")
  def summary = rowTypeIs("RIFee")
  def prepay = rowTypeIs("Fee")
  def usg = where(s"`lineItem/LineItemType` IN ('Usage', 'DiscountedUsage')")

  /**
    * Helpers to select columns.
    */
  def riCols = select(RIColumns: _*)
  def riCostCols: CUR = {
    val cols = List(RICoreColumns, RICostColumns).flatten.distinct
    select(cols: _*)
  }

  /**
    * Helpers to export query results to files.
    * Usually for analyzing CURs, we expect to export result to a single file.
    * It is very slow to export results from a large CUR directly by repartitioning query to 1 partition.
    * One way to solve it is to write results to multiple parts, read those temp files and then repartition
    * the dataset into one. The temp result is stored in Parquet format.
    */
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

  /**
    * Helper to compose a new column `analysisPoint` from `product/region`, `product/operatingSystem`,
    * `product/instanceType`, `product/tenancy`
    * @return
    */
  def withAnalysisPoint(delimiter: String): CUR = {
    val composeAnalysisPoint = udf((instType: String,
                                    region: String,
                                    tenancy: String,
                                    os: String) => Array(instType, region, tenancy, os).mkString(delimiter))
    withColumn("analysisPoint", composeAnalysisPoint(
      col("product/region"),
      col("product/instanceType"),
      col("product/operatingSystem"),
      col("product/tenancy")))
  }
  def withAnalysisPoint: CUR = withAnalysisPoint(":")

  /**
    * Aggregate amortization cost per analysis point.
    * @return
    */
  def amortCostPerAnalysisPoint: CUR = withAnalysisPoint.aggAfterGroupBy(sum("reservation/AmortizedUpfrontCostForUsage"), "analysisPoint")
}
