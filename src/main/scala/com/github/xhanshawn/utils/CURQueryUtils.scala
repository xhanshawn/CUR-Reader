package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.CUR
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import com.github.xhanshawn.utils.CURColumnsDefinitions._

trait CURQueryUtils extends LoggerHelper {

  /**
    * Abstract methods to support DataFrame query chaining over CUR case class.
    */
  def curRows: DataFrame
  def initWithDF(df: DataFrame): CUR
  def rows = curRows /* Sometimes I hate typing  */
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
    cols.headOption match {
      case Some(headCol) => {
        val df = curRows.select(headCol, cols.tail :_*)
        return initWithDF(df)
      }
      case _ => throw new IllegalArgumentException("Empty columns list to select")
    }
  }
  def withColumn(str: String, column: Column): CUR = {
    val df = curRows.withColumn(str, column)
    initWithDF(df)
  }
  def drop(colNames: String*): CUR = {
    val df = curRows.drop(colNames: _*)
    initWithDF(df)
  }
  def orderBy(cols: String*): CUR = {
    log.info(s"added orderBy ${cols.mkString(", ")}")
    cols.headOption match {
      case Some(headCol) => {
        val df = curRows.orderBy(headCol, cols.tail :_*)
        return initWithDF(df)
      }
      case _ => throw new IllegalArgumentException("Empty columns list to orderBy")
    }
  }

  /**
    * Different shows
    */
  def show(num: Int, truncate: Boolean): Unit = {
    curRows.show(num, truncate)
  }
  def show(num: Int): Unit = {
    curRows.show(num)
  }
  def show(truncate: Boolean): Unit = {
    curRows.show(truncate)
  }
  def show(): Unit = {
    curRows.show()
  }

  def first(): Unit = {
    curRows.first()
  }
  def limit(n: Int = 1): CUR = {
    val df = curRows.limit(n)
    initWithDF(df)
  }



  /**
    * Combined groupBy with agg functions together.
    * @param aggExp
    * @param groupCols
    * @return
    */
  def aggAfterGroupBy(aggExp: Column, groupCols: String*): CUR = {
    val df = curRows.groupBy(groupCols.map(col): _*).agg(aggExp)
    initWithDF(df)
  }

  /**
    * cool function to merge columns to a new col by mkString with a delimiter.
    * @param cols      a list of columns you want to merge
    * @param newCol    the new column name you want to give
    * @param delimiter the delimiter you want to use to merge the columns.
    * @param f         function to merge the col values.
    * @return
    */
  def mergeColumns(cols: Seq[String], newCol: String, delimiter: String, f: (Seq[Any], String) => String): CUR = {
    val merge = udf((cols: Seq[Any]) => f(cols, delimiter))
    withColumn(newCol, merge(array(cols.map(col(_)): _*)))
  }

  /**
    * Default implementation for the mergeColumns with mkString function.
    * @param cols
    * @param newCol
    * @param delimiter
    * @return
    */
  def mergeColumns(cols: Seq[String], newCol: String, delimiter: String): CUR = {
    mergeColumns(cols, newCol, delimiter, (cols, delimiter) => cols.mkString(delimiter))
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
    * I just don't like long names.
    */
  def print(): Unit = {
    printRows(curRows)
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
    * Helpers to query CUR rows by operation and usage type.
    */
  def compute = where("`lineItem/Operation` LIKE 'RunInstances%' AND `lineItem/UsageType` LIKE '%Usage:%'")

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
  def usg = where("`lineItem/LineItemType` IN ('Usage', 'DiscountedUsage')")

  /**
    * Helpers to select columns.
    */
  def riCols = select(RIColumns: _*)
  def riCostCols: CUR = {
    val cols = List(RICoreColumns, RICostColumns).flatten.distinct
    select(cols: _*)
  }
  def riModelCols = select(RIModelColumns: _*)

  /**
    * Helpers to export query results to files.
    * Usually for analyzing CURs, we expect to export result to a single file.
    * It is very slow to export results from a large CUR directly by repartitioning query to 1 partition.
    * One way to solve it is to write results to multiple parts, read those temp files and then repartition
    * the dataset into one. The temp result is stored in Parquet format.
    */
  val TempDFFileName = "tmp-df-file"
  def write(dfToWrite: DataFrame, partitionNum: Int = 1): DataFrameWriter[Row] = {
    if (partitionNum == 1) {
      val tempFile = HDFSUtils.getTempFilePath(TempDFFileName).toString
      HDFSUtils.clearTempFile(tempFile)
      dfToWrite.write.parquet(tempFile)
      val spark = sparkSessionBuilder.build()
      val df = spark.read.parquet(tempFile)
      df.repartition(partitionNum).write
    } else {
      dfToWrite.repartition(partitionNum).write
    }
  }
  def write: DataFrameWriter[Row] = write(curRows, 1)

  /**
    * Helper to compose a new column `analysisPoint` from `product/region`, `product/operatingSystem`,
    * `product/instanceType`, `product/tenancy`
    * @return
    */
  def withAnalysisPoint(delimiter: String): CUR = {
    mergeColumns(
      Seq("product/region", "product/instanceType", "product/operatingSystem", "product/tenancy"), "analysisPoint", delimiter)
  }
  def withAnalysisPoint: CUR = withAnalysisPoint(":")

  /**
    * Aggregate amortization cost per analysis point.
    * @return
    */
  def amortCostPerAnalysisPoint: CUR = withAnalysisPoint.aggAfterGroupBy(sum("reservation/AmortizedUpfrontCostForUsage"), "analysisPoint")
}
