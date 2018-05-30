package com.github.xhanshawn.utils

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{AmazonS3Exception, ListObjectsRequest, ObjectListing, S3ObjectSummary}

import scala.annotation.tailrec
import scala.io.Source

trait S3Utils extends LoggerHelper {
  lazy val s3 = new AmazonS3Client

  /**
    * Read S3 object directly into string. It should only be used for loading small files.
    * @param bucket
    * @param key
    * @return
    */
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

  /**
    * Downloads Big GZ files from S3.
    * @param bucket
    * @param key
    * @return
    */
  def downloadGZFromS3(bucket: String, key: String): String = {
    try {
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
    } catch {
      case ex: AmazonS3Exception => {
        log.warn(ex.getMessage)
        null
      }
    }
  }

  def listObjects(bucket: String, prefix: String): Seq[S3ObjectSummary] = {
    @tailrec
    def listPerMarker(nextMarker: String, prevSums: Seq[S3ObjectSummary]): Seq[S3ObjectSummary] = {
      val request = new ListObjectsRequest(bucket, prefix, nextMarker, null, 1000)
      val listResult = s3.listObjects(request)
      import scala.collection.JavaConversions._
      val sums = prevSums ++ listResult.getObjectSummaries
      listResult.getNextMarker() match {
        case null => sums
        case marker: String => listPerMarker(nextMarker, sums)
      }
    }
    listPerMarker(null, List())
  }
}
