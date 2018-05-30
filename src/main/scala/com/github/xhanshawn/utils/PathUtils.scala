package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURPath}

object PathUtils extends S3Utils {

  /* Reused constant for the regex of month span like "20180101-20180201 */
  private val monthSpanRegex = raw"\d{8}-\d{8}"

  /* Reused constant for the regex of manifest file like "report-Manifest.json". */
  private val ManifestFileRegex = raw"([^/]*)-Manifest.json"

  /* Regex for matching CUR Root path. */
  private val CURRootPathRegex = raw"(s3[an]*|file)://(.*)/($monthSpanRegex)/([-a-f0-9]*)(/?)".r

  /* Regex for matching CUR Files. */
  private val CURFileRegex     = raw"(s3[an]*|file)://(.*)/($monthSpanRegex)/([-a-f0-9]*)/(.*.gz)".r

  /* Regex for matching CUR Manifest files. */
  private val CURManifestRegex = s"(s3[an]*|file)://(.*)/($monthSpanRegex)/([-a-f0-9]*)/$ManifestFileRegex".r

  /* Regex for matching Root CUR Root path. */
  private val RootManifestRegex = s"(s3[an]*|file)://(.*)/($monthSpanRegex)/$ManifestFileRegex".r

  /* Regex for matching object keys of Manifests. */
  private val ManifestObjRegex = s"(.*)/($monthSpanRegex)/([-a-f0-9]*)/$ManifestFileRegex".r

  /* Regex for matching object keys of root Manifests. */
  private val RootManifestObjRegex = s"(.*)/($monthSpanRegex)/$ManifestFileRegex".r

  abstract class BaseSys(name: String) {
    val root: String = s"$name://".intern()
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
      case CURRootPathRegex(sys, reportPath, monthSpan, "",  "") => {
        val curPath = CURPath(sys, reportPath, monthSpan, null, null)
        findCURManifest(curPath)
      }
      case CURRootPathRegex(sys, reportPath, monthSpan, assemblyId,  "") => {
        val curPath = CURPath(sys, reportPath, monthSpan, assemblyId, null)
        findCURManifest(curPath)
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

  def findCURManifest(curPath: CURPath): CURPath = {
    if (curPath.fromS3 && !curPath.hasManifest) {
      val sums = listObjects(curPath.bucket, curPath.prefix)
      val objKeys = sums.map(sum => sum.getKey)
      val manifests = findManifestFromPaths(objKeys, curPath)
      manifests.length match {
        case 0 => {
          log.warn(s"Manifest not found from your path.")
          null
        }
        case 1 => {
          log.warn(s"Found one possible CUR manifest: ${manifests.map(_.manifestPath)}, will load this CUR.")
          manifests(0)
        }
        case _ => {
          log.warn(s"Found multiple possible CUR manifests. Please specify which one is the right one. ${manifests.map(_.manifestPath)}")
          null
        }
      }
    } else curPath
  }

  def findManifestFromPaths(paths: Seq[String], curPath: CURPath): Seq[CURPath] = {
    val manifests = if (curPath.hasAssemblyId) paths.filter(objIsManifest) else paths.filter(isRootManifest)
    manifests.map(getRootManifestCURPath(_, curPath)).filter(_ != null)
  }

  def objIsManifest(objKey: String): Boolean = objKey match {
    case ManifestObjRegex(_, month, assembly, manifest) => true
    case RootManifestObjRegex(_, month, manifest) => true
    case _ => false
  }

  def isRootManifest(path: String): Boolean = path match {
    case RootManifestObjRegex(_, month, manifest) => true
    case _ => false
  }

  def getRootManifestCURPath(path: String, curPath: CURPath): CURPath = path match {
    case ManifestObjRegex(prefix, month, assembly, manifest) => {
      val reportPath = s"${curPath.bucket}/${prefix}"
      if (assembly == curPath.assemblyId &&
          month == curPath.monthSpan && reportPath == curPath.reportPath) {
        CURPath(curPath.sys, curPath.reportPath, month, assembly, manifest)
      } else null
    }
    case RootManifestObjRegex(prefix, month, manifest) => {
      val reportPath = s"${curPath.bucket}/${prefix}"
      if (month == curPath.monthSpan && reportPath == curPath.reportPath) {
        CURPath(curPath.sys, curPath.reportPath, month, null, manifest)
      } else null
    }
    case _ => null
  }
}
