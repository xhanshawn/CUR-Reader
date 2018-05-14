package com.github.xhanshawn.reader

import org.apache.spark.sql.{Dataset}

case class CUR(curPath: CURPath, curManifest: CURManifest) {

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
  var curRows: Dataset[CURRow] = null
}
