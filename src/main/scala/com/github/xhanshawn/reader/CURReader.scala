package com.github.xhanshawn.reader

import com.github.xhanshawn.utils._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object CURReader extends LoggerHelper {
  def read(spark: SparkSession, paths: Seq[String], config: ReaderConfig = defaultConfig): DataFrame = {
    configLogger(spark.sparkContext)

    readConfig(config)
    HDFSUtils.setHDFSConfig(spark)

    import spark.implicits._
    val curPaths = spark.createDataset(paths).flatMap{ path => Seq(PathUtils.parseCURPath(path)) }

    val manifests = ManifestLoader.load(spark, curPaths)

    val curs = manifests.joinWith(curPaths, manifests.col("reportName") === curPaths.col("reportName"))
                        .withColumnRenamed("_1", "curManifest")
                        .withColumnRenamed("_2", "curPath")
                        .as[CUR]
    import spark.implicits._
    val totalNumOfParts = curs.map(_.numParts).collect().foldLeft(0)(_ + _)
    println(s"Loading $totalNumOfParts CUR parts.")
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

  def clearTempFiles(spark: SparkSession): Unit = {
    HDFSUtils.setHDFSConfig(spark)
    HDFSUtils.deleteAllTempFiles() match {
      case Success(true) => log.warn("Temp files are deleted.")
      case Success(false) => log.warn("Temp files are not deleted but no exception.")
      case Failure(ex) => throw ex
    }
  }

  private def readConfig(config: ReaderConfig): Unit = {
    runningConfig.readFull = config.readFull
    runningConfig.usingAWSAPI = config.usingAWSAPI
    runningConfig.inferSchema = config.inferSchema
  }
}
