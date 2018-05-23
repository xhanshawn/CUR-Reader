package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURPart, runningConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.{Failure, Success, Try}


object CURPartLoader extends S3Utils {

  val MaxPartitionSize = 4

  /**
    * Loads a set of CURParts. Either using AWSAPI or HadoopAWS.
    * @param spark
    * @param curParts
    * @param useAWSAPI Flag for using AWSAPI or not.
    * @return          Dataframe of CUR files.
    */
  def load(spark: SparkSession, curParts: Dataset[CURPart], useAWSAPI: Boolean): DataFrame = {
    if (useAWSAPI) {
      HDFSUtils.clearTempFiles(spark)
      handleDownloadingError[DataFrame](downloadUsingAWSAPI(spark, curParts)) match {
        case Success(df) => df
        case Failure(ex) => throw ex
      }
    } else loadUsingHadoopAWS(spark, curParts)
  }

  /**
    * Load CUR parts using HadoopAWS library. CUR files will not be downloaded.
    * @param spark
    * @param curParts
    * @return
    */
  def loadUsingHadoopAWS(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val curPartPaths = curParts.map(_.fullPath).collect()
    spark.read
      .option("header", true)
      //      .option("inferSchema", true)
      .csv(curPartPaths: _*)
  }

  /**
    * Downloads CUR files using AWS API. It actually downloads files to the temp dir of spark.
    * @param spark
    * @param curParts
    * @return
    */
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

  /**
    * Handles the downloading errors run inside the method.
    *
    * @param f  Downloading function is supposed to run.
    * @tparam T Result type.
    * @return
    */
  def handleDownloadingError[T](f: => T): Try[T] = {
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

  /**
    * Not being used. It was an attempt to use parallelize() to download files in parallel.
    * @param spark
    * @param curParts
    * @return
    */
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

  @deprecated("Out of Memory issue", "0.1")
  def load(spark: SparkSession, curParts: Seq[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = spark.sparkContext.parallelize(curParts).flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey)).toDS()
    spark.read
      .option("header", true)
      .csv(linesDS)
  }

  @deprecated("Out of Memory issue", "0.1")
  def loadFromAWSAPI(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = curParts
      .repartition(MaxPartitionSize)
      .flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey))
    spark.read
      .option("header", true)
      .csv(linesDS)
  }
}
