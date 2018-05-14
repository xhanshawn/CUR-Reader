package com.github.xhanshawn.utils

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3Client

import scala.io.Source

trait S3Utils {
  lazy val s3 = new AmazonS3Client

  def readFromS3ByString(bucket: String, key: String): String = {
    Source.fromInputStream(s3.getObject(bucket, key).getObjectContent: InputStream).mkString
  }

  def readGZFromS3ByLine(bucket: String, key: String): Iterator[String] = {
    Source.fromInputStream(new GZIPInputStream(s3.getObject(bucket, key).getObjectContent: InputStream)).getLines()
  }

  def readGZFromS3ByString(bucket: String, key: String): String = {
    Source.fromInputStream(new GZIPInputStream(s3.getObject(bucket, key).getObjectContent: InputStream)).mkString
  }
}
