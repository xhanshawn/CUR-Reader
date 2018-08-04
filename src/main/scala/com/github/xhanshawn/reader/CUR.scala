package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.{CURPartLoader, CURQueryUtils, LoggerHelper}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class CUR(curPath: CURPath, curManifest: CURManifest, var curRows: Dataset[Row]) extends LoggerHelper with CURQueryUtils {

  val isPathMatching: Boolean = (curPath.reportPrefix == curManifest.reportPrefix.getOrElse(""))
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

  /**
    * Loads CUR Rows from CUR Parts. If the cache configuration is set to true, it will download all the
    * CUR parts to temp directory. Be cautious when you call this.
    * @param spark
    * @return
    */
  def loadCurRows(spark: SparkSession): Boolean = {
    if (curRows != null) return true
    val parts =
      if (runningConfig.readFull) {
        log.warn(s"loading ${curParts.length} CUR part files.")
        curParts
      } else {
        log.warn("readFull config is false. loading the first CUR part file.")
        firstPart
      }
    import spark.implicits._
    val partsDS = spark.createDataset(parts)
    curRows = CURPartLoader.load(spark, partsDS, useAWSAPI)
    true
  }

  override def initWithDF(df: DataFrame): CUR = CUR(curPath, curManifest, df)
}
