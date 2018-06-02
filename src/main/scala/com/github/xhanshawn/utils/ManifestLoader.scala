package com.github.xhanshawn.utils

import com.github.xhanshawn.reader._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{struct, to_timestamp}

object ManifestLoader extends S3Utils {
  def load(spark: SparkSession, curPaths: Dataset[CURPath]): Dataset[CURManifest] = {
    if (curPaths.filter(_.useAWSAPI).count() > 0) {
      loadFromAWSAPI(spark, curPaths)
    } else loadFromS3URL(spark, curPaths)
  }

  def loadFromS3URL(spark: SparkSession, curPaths: Dataset[CURPath]): Dataset[CURManifest] = {
    import spark.implicits._
    val fullPaths = curPaths.map(path => path.manifestPath).collect()
    log.warn(s"loading Manifests: ${fullPaths.mkString(",")}")
    val df = spark.read
              .option("multiLine", "true")
              .json(fullPaths: _*)
    toCURManifests(spark, df)
  }

  def loadFromAWSAPI(spark: SparkSession, curPaths: Dataset[CURPath]): Dataset[CURManifest] = {
    import spark.implicits._
    val validPaths = curPaths.filter(_.fromS3)
    val jsonDS = validPaths.repartition(validPaths.count().toInt).map(path => readFromS3ByString(path.bucket, path.manifestKey))
    val df = spark.read.json(jsonDS)
    toCURManifests(spark, df)
  }

  @deprecated("", "0.1")
  def loadFromAWSAPI(spark: SparkSession, bucket: String, keys: Seq[String]): Dataset[CURManifest] = {
    import spark.implicits._
    val jsonDS = spark.createDataset(keys).map(readFromS3ByString(bucket, _))
    val df = spark.read.json(jsonDS)
    toCURManifests(spark, df)
  }

  def toCURManifests(spark: SparkSession, manifestDF: DataFrame): Dataset[CURManifest] = {
    import spark.implicits._
    manifestDF.withColumn("billingPeriod", struct(
      to_timestamp($"billingPeriod.start", "yyyyMMdd'T'HHmmss.SSS'Z'").as("start"),
      to_timestamp($"billingPeriod.end", "yyyyMMdd'T'HHmmss.SSS'Z'").as("end")
    ))
      .withColumn("formatData", struct($"charset", $"compression", $"contentType"))
      .as[CURManifest]
  }
}
