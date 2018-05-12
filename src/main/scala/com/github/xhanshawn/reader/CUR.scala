package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.PathUtils.CURPath
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CUR(curPath: CURPath, curManifest: CURManifest) {

  val isPathMatching: Boolean = (curPath.prefix == curManifest.reportPrefix)

  val curParts: Seq[CURPart] = {
    if (isPathMatching) curManifest.reportKeys.map(CURPart(curPath.bucket, _))
    else curManifest.reportKeys.map {
      key => {
        val name = key.split("/").last
        CURPart(curPath.bucket, s"${curPath.s3prefix}/${name}")
      }
    }
  }
}
