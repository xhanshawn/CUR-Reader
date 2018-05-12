package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.CURManifest
import com.github.xhanshawn.utils.PathUtils.CURPath
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{struct, to_timestamp}

object ManifestLoader extends S3Utils {
  def load(spark: SparkSession, curPaths: Dataset[CURPath]): Dataset[CURManifest] = {
    import spark.implicits._
    val validPaths = curPaths.filter(_.fromS3)
    val paths = validPaths.map(path => (path.bucket, path.manifestKey))
    val ds = curPaths.map(path => readFromS3ByString(path.bucket, path.manifestKey))
    toCURManifests(spark, ds)
  }

  def load(spark: SparkSession, path: String): Dataset[CURManifest] = {
    import spark.implicits._
    val curPaths = spark.createDataset(List(path)).flatMap(path => Seq(PathUtils.parseCURPath(path)))
    load(spark, curPaths)
  }

  def load(spark: SparkSession, bucket: String, keys: Seq[String]): Dataset[CURManifest] = {
    import spark.implicits._
    val jsonDS = spark.createDataset(keys).map(readFromS3ByString(bucket, _))
    toCURManifests(spark, jsonDS)
  }

  def toCURManifests(spark: SparkSession, manifestDS: Dataset[String]): Dataset[CURManifest] = {
    import spark.implicits._
    val df = spark.read.json(manifestDS)
    df.withColumn("billingPeriod", struct(
      to_timestamp($"billingPeriod.start", "yyyyMMdd'T'HHmmss.SSS'Z'").as("start"),
      to_timestamp($"billingPeriod.end", "yyyyMMdd'T'HHmmss.SSS'Z'").as("end")
    ))
      .withColumn("formatData", struct($"charset", $"compression", $"contentType"))
      .as[CURManifest]
  }
}
