package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.{ManifestLoader, PathUtils}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SparkSession}

object CURReader {

  def read(spark: SparkSession, paths: Seq[String]): Unit = {
    import spark.implicits._
    val curPaths = spark.createDataset(paths)
                        .flatMap(path => Seq(PathUtils.parseCURPath(path)))
    val manifests = ManifestLoader.load(spark, curPaths)

    val curs = manifests.joinWith(curPaths, manifests.col("reportName") === curPaths.col("reportName"))
                        .withColumnRenamed("_1", "curManifest")
                        .withColumnRenamed("_2", "curPath")
                        .as[CUR]
    println()
  }


  def read(spark: SparkSession, path: String): Unit = {
    read(spark, List(path))
  }
}
