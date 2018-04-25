package com.github.xhanshawn.utils

object PathUtils {

  private val CURRootPathRegex = raw"(s3[an]*|file)://[-_/a-zA-Z]+/(\d{8}-\d{8})/([-a-f0-9]*)(/?)".r
  private val CURManifestRegex = raw"(s3[an]*|file)://[-_/a-zA-Z]+/(\d{8}-\d{8})/([-a-f0-9]*)/([-a-zA-Z]+.json)".r

  case class CURPath(source: String, monthSpan: String, assemblyId: String, manifest: Option[String]) {
    def hasManifst: Boolean = (manifest != None)
  }

  def parseCURPath(path: String): Option[CURPath] = {
    path match {
      case CURRootPathRegex(source, monthSpan, assemblyId,  _) => {
        Some(CURPath(source, monthSpan, assemblyId, None))
      }

      case CURManifestRegex(source, monthSpan, assemblyId, manifest) => {
        Some(CURPath(source, monthSpan, assemblyId, Some(manifest)))
      }

      case _ => {
        println("Invalid CUR path")
        None
      }
    }
  }
}
