package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURPart, runningConfig}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object CURPartLoader extends S3Utils {

  def load(spark: SparkSession, curParts: Seq[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = spark.sparkContext.parallelize(curParts).flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey)).toDS()
    spark.read
      .option("header", true)
//      .option("inferSchema", true)
      .csv(linesDS)
  }

  def load(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    if (runningConfig.usingAWSAPI) loadFromAWSAPI(spark, curParts)
    else loadUsingHadoopAWS(spark, curParts)
  }

  def loadFromAWSAPI(spark: SparkSession, curParts: Dataset[CURPart]): DataFrame = {
    import spark.implicits._
    val linesDS = curParts
//      .repartition(4)
      .flatMap(part => readGZFromS3ByLine(part.bucket, part.reportKey))
    spark.read
      .option("header", true)
      //      .option("inferSchema", true)
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
}
