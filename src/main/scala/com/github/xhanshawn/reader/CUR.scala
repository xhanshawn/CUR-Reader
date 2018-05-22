package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.{CURPartLoader, LoggerHelper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class CUR(curPath: CURPath, curManifest: CURManifest) extends LoggerHelper {

  val isPathMatching: Boolean = (curPath.reportPrefix == curManifest.reportPrefix)

  val isFromS3 = curPath.fromS3
  val curParts: Seq[CURPart] = {
    if (isPathMatching) curManifest.reportKeys.map((reportKey: String) => CURPart(curPath.sysType.root, curPath.bucket, reportKey))
    else curManifest.reportKeys.map {
      key => {
        val name = key.split("/").last
        CURPart(curPath.sysType.root, curPath.bucket, s"${curPath.prefix}/${name}")
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
    curRowsDF = CURPartLoader.load(spark, partsDS)
    true
  }

  def curRows: DataFrame = {
    curRowsDF
  }
}
