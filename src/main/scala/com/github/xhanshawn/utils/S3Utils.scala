package com.github.xhanshawn.utils

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.FileSystem

import scala.io.Source

trait S3Utils extends LoggerHelper {
  lazy val s3 = new AmazonS3Client

  def readFromS3ByString(bucket: String, key: String): String = {
    Source.fromInputStream(s3.getObject(bucket, key).getObjectContent: InputStream).mkString
  }

  @deprecated("Out of Memory issue", "0.1")
  def readGZFromS3ByLine(bucket: String, key: String): Iterator[String] = {
    Source.fromInputStream(new GZIPInputStream(s3.getObject(bucket, key).getObjectContent: InputStream)).getLines()
  }

  @deprecated("Out of Memory issue", "0.1")
  def readGZFromS3ByString(bucket: String, key: String): String = {
    Source.fromInputStream(new GZIPInputStream(s3.getObject(bucket, key).getObjectContent: InputStream)).mkString
  }

  def downloadGZFromS3(bucket: String, key: String): String = {
    val s3Object = s3.getObject(bucket, key)

    /* TODO::Check local file meta data against S3 object meta data to decide if we should skip or not. */
    val localFilePath = ensureS3PathToFilePath(key)
    if (HDFSUtils.fileExists(localFilePath)) {
      log.warn(s"Skip downloading: ${key}, because file: ${localFilePath}.")
      return HDFSUtils.getTempFilePath(localFilePath).toString()
    } else {
      val input = new GZIPInputStream(s3Object.getObjectContent: InputStream)
      val output = HDFSUtils.createTempFile(localFilePath)
      HDFSUtils.copyToFile(input, output)
      HDFSUtils.getTempFilePath(localFilePath).toString()
    }
  }

}
