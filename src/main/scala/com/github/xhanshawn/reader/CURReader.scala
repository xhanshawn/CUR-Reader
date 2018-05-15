package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.{CURPartLoader, Logger, ManifestLoader, PathUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CURReader extends Logger {
  def read(spark: SparkSession, paths: Seq[String], config: ReaderConfig = defaultConfig): DataFrame = {
    configLogger(spark.sparkContext)

    readConfig(config)

    import spark.implicits._
    val curPaths = spark.createDataset(paths).flatMap{ path => Seq(PathUtils.parseCURPath(path)) }

    val manifests = ManifestLoader.load(spark, curPaths)

    val curs = manifests.joinWith(curPaths, manifests.col("reportName") === curPaths.col("reportName"))
                        .withColumnRenamed("_1", "curManifest")
                        .withColumnRenamed("_2", "curPath")
                        .as[CUR]
    import spark.implicits._
    val curParts =
      if (runningConfig.readFull) curs.flatMap(cur => cur.curParts)
      else curs.flatMap(cur => cur.firstPart)

    val curRows = CURPartLoader.load(spark, curParts)
    println()
    curRows
  }


  def read(spark: SparkSession, path: String): DataFrame = {
    read(spark, List(path))
  }

  private def readConfig(config: ReaderConfig): Unit = {
    runningConfig.readFull = config.readFull
    runningConfig.usingAWSAPI = config.usingAWSAPI
    runningConfig.inferSchema = config.inferSchema
  }
}
