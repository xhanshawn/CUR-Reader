package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURPart, runningConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}


object CURPartLoader extends S3Utils {

  val MAX_PARTITION_SIZE = 4

  @deprecated("Out of Memory issue", "0.1")
  def load(spark: SparkSession, curParts: Seq[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = spark.sparkContext.parallelize(curParts).flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey)).toDS()
    spark.read
      .option("header", true)
//      .option("inferSchema", true)
      .csv(linesDS)
  }

  def load(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    if (runningConfig.usingAWSAPI) {
      HDFSUtils.clearTempFiles(spark)
      handleDownloadingError[DataFrame](downloadUsingAWSAPI(spark, curParts)) match {
        case Success(df) => df
        case Failure(ex) => throw ex
      }
    } else loadUsingHadoopAWS(spark, curParts)
  }

  @deprecated("Out of Memory issue", "0.1")
  def loadFromAWSAPI(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = curParts
      .repartition(MAX_PARTITION_SIZE)
      .flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey))
    spark.read
      .option("header", true)
      .csv(linesDS)
  }

  def loadUsingHadoopAWS(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val curPartPaths = curParts.map(_.fullPath).collect()
    spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv(curPartPaths: _*)
  }

  def downloadUsingAWSAPI(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val curPartPaths = curParts.map(_.fullPath).collect()
    val totalNumOfParts = curPartPaths.length
    val filePaths = curParts.repartition(totalNumOfParts)
                            .map(part => downloadGZFromS3(part.bucket, part.reportKey)).collect()
    spark.read
      .option("header", true)
      .csv(filePaths: _*)
  }

  def downloadUsingAWSAPIUsingRDD(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    val parts = curParts.collect()
    val totalNumOfParts = parts.length
    val sc = spark.sparkContext
    val filePaths = sc.parallelize(parts, totalNumOfParts)
      .map(part => downloadGZFromS3(part.bucket, part.reportKey)).collect()
    spark.read
      .option("header", true)
      .csv(filePaths: _*)
  }

  def handleDownloadingError[C](f: => C): Try[C] = {
    try {
      val df = f
      Success(df)
    } catch {
      case ex: Throwable => {
        log.warn(s"Error when downloading CURs: ${ex.getMessage}")
        log.warn(ex.getStackTrace)
        return Failure(ex)
      }
    }
  }
}
