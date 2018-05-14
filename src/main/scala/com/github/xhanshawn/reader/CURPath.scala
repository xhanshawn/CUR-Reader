package com.github.xhanshawn.reader

import java.nio.file.Paths

import com.github.xhanshawn.utils.PathUtils.{FileSysType}

case class CURPath(sys: String, reportPath: String, monthSpan: String, assemblyId: String, reportName: String) extends Serializable with FileSysType {

  val manifest = if(hasManifest) s"$reportName-Manifest.json" else null
  val sysType = getSysType(sys)
  val curDirectory: String = s"${sysType.root}$reportPath/$monthSpan/$assemblyId"
  val fromS3: Boolean = (S3Systems.contains(sysType))

  def manifestPath: String = runIfManifest(getManifestPath)
  private def getManifestPath: String = Paths.get(curDirectory, manifest).toString()

  def hasManifest: Boolean = reportName != null
  def runIfManifest[T](f: => T): T = {
    if(hasManifest) f
    else {
      throw new IllegalArgumentException("Manifest file is not in your path. Finding Manifest is not supported yet!")
    }
  }

  val bucket: String = {
    if (fromS3) {
      val index = reportPath.indexOf("/")
      reportPath.substring(0, index)
    } else null
  }

  def manifestKey: String = runIfManifest(getManifestKey)
  private def getManifestKey: String = Paths.get(prefix, manifest).toString()

  def reportPrefix: String = {
    val index = reportPath.indexOf("/")
    reportPath.substring(index + 1)
  }

  def prefix: String = s"$reportPrefix/$monthSpan/$assemblyId"
}
