package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.{CURPartLoader, CURQueryUtils, LoggerHelper}
import org.apache.log4j.Logger
import org.apache.spark.sql._

case class CUR(curPath: CURPath, curManifest: CURManifest) extends LoggerHelper with CURQueryUtils {

  val isPathMatching: Boolean = (curPath.reportPrefix == curManifest.reportPrefix)
  val useAWSAPI = curPath.useAWSAPI || runningConfig.usingAWSAPI

  val isFromS3 = curPath.fromS3
  val curParts: Seq[CURPart] = {
    if (isPathMatching) curManifest.reportKeys.map((reportKey: String) => CURPart(curPath.sysType.root, curPath.bucket, reportKey))
    else curManifest.reportKeys.map {
      key => {
        val name = key.split("/").last
        if (curPath.hasAssemblyId) {
          CURPart(curPath.sysType.root, curPath.bucket, s"${curPath.prefix}/${name}")
        } else {
          CURPart(curPath.sysType.root, curPath.bucket, s"${curPath.prefix}/${curManifest.assemblyId}/${name}")
        }
      }
    }
  }

  val firstPart: Seq[CURPart] = curParts.filter(part => part.reportKey.contains(curManifest.firstPartName))
  val numParts: Int = curParts.length

  private var curRowsDF: DataFrame = null

  def loadCurRows(spark: SparkSession): Boolean = {
    val parts =
      if (runningConfig.readFull) {
        log.warn(s"loading ${curParts.length} CUR part files.")
        curParts
      } else {
        log.warn(s"readFull config is false. loading the first CUR part file.")
        firstPart
      }
    import spark.implicits._
    val partsDS = spark.createDataset(parts)
    curRowsDF = CURPartLoader.load(spark, partsDS, useAWSAPI)
    true
  }

  override def curRows: DataFrame = {
    curRowsDF
  }
  override def where(condition: String): CUR = {
    log.info(s"added where clause ${condition}")
    tmpDF.where(condition)
    this
  }
  override def select(cols: String*): CUR = {
    log.info(s"added select clause ${cols.mkString(", ")}")
    tmpCURDF = tmpDF.select(cols.head, cols.tail :_*)
    this
  }
}
