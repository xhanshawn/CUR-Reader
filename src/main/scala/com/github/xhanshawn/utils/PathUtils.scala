package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURPath}

object PathUtils {

  private val CURRootPathRegex = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)(/?)".r
  private val CURManifestRegex = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)/(.*)-Manifest.json".r
  private val CURFileRegex     = raw"(s3[an]*|file)://(.*)/(\d{8}-\d{8})/([-a-f0-9]*)/(.*.gz)".r

  abstract class BaseSys(name: String) {
    val root: String = s"$name://"
  }

  trait FileSysType {
    case object S3Sys extends BaseSys("s3")
    case object S3aSys extends BaseSys("s3a")
    case object S3nSys extends BaseSys("s3n")
    case object FileSys extends BaseSys("file")

    def getSysType(sysTypeString: String): BaseSys = sysTypeString match {
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
      case CURFileRegex(sys, reportPath, monthSpan, assemblyId, _) => {
        CURPath(sys, reportPath, monthSpan, assemblyId, null)
      }
      case _ => {
        throw new IllegalArgumentException("Invalid CUR path")
      }
    }
  }
}
