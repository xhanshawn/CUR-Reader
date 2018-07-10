package com.github.xhanshawn.reader

import com.github.xhanshawn.utils._
import org.apache.spark.sql.{SparkSession}

object CURReader extends LoggerHelper {
  var config: ReaderConfig = defaultConfig

  def read(spark: SparkSession, paths: Seq[String]): Seq[CUR] = {
    configLogger(spark.sparkContext)

    readConfig(config)
    HDFSUtils.setHDFSConfig(spark)

    /**
      * Only clear temp files created by CUR Reader.
      * It is easy to blow up if we don't do this before each load.
      */
    clearTempFiles(spark)
    import spark.implicits._
    val curPaths = spark.createDataset(paths.flatMap{ path => Seq(PathUtils.parseCURPath(path)) })
    if(curPaths.count() > 1 && runningConfig.usingAWSAPI) {
      log.warn(
        """Loading CUR using AWS API requires downloading CUR files. Temp CUR files will not be retained
          |when loading multiple CURs to avoid disk space issues.
        """.stripMargin)
    }

    val manifests = ManifestLoader.load(spark, curPaths)


    val pathsWithManifests = manifests.joinWith(curPaths, manifests.col("reportName") === curPaths.col("reportName")).collect()
    val curs = pathsWithManifests.map {
      case (manifest, path) => CUR(path, manifest, null)
      case _ => null
    }

    val totalNumOfParts = curs.map(_.numParts).foldLeft(0)(_ + _)
    println(s"Total num of cur parts: $totalNumOfParts ")
    curs.foreach(cur => cur.loadCurRows(spark))
    curs
  }

  def read(spark: SparkSession, path: String): CUR = {
    val curs = read(spark, List(path))
    if(curs.length == 1) curs(0)
    else null
  }

  def clearTempFiles(spark: SparkSession): Unit = {
    HDFSUtils.clearTempFiles(spark)
  }

  private def readConfig(config: ReaderConfig): Unit = {
    runningConfig.readFull = config.readFull
    runningConfig.usingAWSAPI = config.usingAWSAPI
    runningConfig.inferSchema = config.inferSchema
  }
}
