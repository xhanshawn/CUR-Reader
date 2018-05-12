package com.github.xhanshawn.utils

import java.nio.file.Paths

object PathUtils {

  private val CURRootPathRegex = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)(/?)".r
  private val CURManifestRegex = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)/(.*)-Manifest.json".r
  private val CURFileRegex     = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)/(.*.gz)".r

  case class CURPath(sys: String, reportPath: String, monthSpan: String, assemblyId: String, reportName: String) extends FileSysType {
    def hasManifst: Boolean = reportName != null
    val manifest = if(hasManifst) s"$reportName-Manifest.json" else null
    def prefix: String = s"$reportPath/$monthSpan/$assemblyId"
    def rootPath: String = s"$sys://" + prefix

    def manifestKey: String = runIfManifest(getManifestKey)
    private def getManifestKey: String = Paths.get(s3prefix, manifest).toString()

    def manifestPath: String = runIfManifest(getManifestPath)
    private def getManifestPath: String = Paths.get(rootPath, manifest).toString()

    def bucket: String = {
      if(fromS3) {
        val index = prefix.indexOf("/")
        prefix.substring(0, index)
      } else null
    }

    def s3reportPath: String = {
      if(fromS3) {
        val index = reportPath.indexOf("/")
        reportPath.substring(index)
      } else null
    }

    def s3prefix: String = s"$s3reportPath/$monthSpan/$assemblyId"

    def runIfManifest[T](f: => T): T = {
      if(hasManifst) f
      else {
        throw new IllegalArgumentException("Manifest file is not in your path. Finding Manifest is not supported yet!")
      }
    }
    val sysType: BaseSys = sysType(sys)
    def fromS3: Boolean = S3Systems.contains(sysType)
  }

  trait S3CURPathUtils {

  }

  trait FileSysType {
    abstract class BaseSys
    case object S3Sys extends BaseSys
    case object S3aSys extends BaseSys
    case object S3nSys extends BaseSys
    case object FileSys extends BaseSys

    def sysType(sysTypeString: String): BaseSys = sysTypeString match {
      case "s3"   => S3Sys
      case "s3a"  => S3aSys
      case "s3n"  => S3nSys
      case "file" => FileSys
    }
    val S3Systems: Set[BaseSys] = Set(S3Sys, S3nSys, S3aSys)

  }

  def parseCURPath(path: String): CURPath = {
    path match {
      case CURRootPathRegex(sys, reportPath, monthSpan, assemblyId,  _) => {
        CURPath(sys, reportPath, monthSpan, assemblyId, null)
      }
      case CURManifestRegex(sys, reportPath, monthSpan, assemblyId, reportName) => {
        CURPath(sys, reportPath, monthSpan, assemblyId, reportName)
      }
      case CURFileRegex(sys, reportPath, monthSpan, assemblyId, _) =>{
        CURPath(sys, reportPath, monthSpan, assemblyId, null)
      }
      case _ => {
        throw new IllegalArgumentException("Invalid CUR path")
      }
    }
  }
}
