package com.github.xhanshawn.utils

import java.io.{BufferedOutputStream, InputStream, OutputStream}
import java.util.zip.GZIPOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait HDFSUtils extends LoggerHelper {
  final private val TEMP_DIR = "/tmp/cur-reader/curs/".intern()
  private var fs: FileSystem = null

  def setHDFSConfig(spark: SparkSession): Unit = fs match {
    case null => {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      fs = FileSystem.get(hadoopConf)
    }
    case _ => log.debug("HDFS System Conf is already set")
  }

  def createTempFile(filePath: String): OutputStream = {
    new BufferedOutputStream(new GZIPOutputStream(fs.create(getTempFilePath(filePath))))
  }

  def getTempFilePath(fileName: String): Path = {
    new Path(TEMP_DIR, fileName)
  }

  def fileExists(filePath: String): Boolean = {
    fs.exists(getTempFilePath(filePath))
  }

  def copyToFile[O <: OutputStream](inputStream: InputStream, outputStream: O): Unit = {
    val buffer = new Array[Byte](65536)
    Stream.continually(inputStream.read(buffer)).takeWhile(_ != -1).foreach(outputStream.write(buffer, 0, _))
    outputStream.close()
  }

  def deleteAllTempFiles(): Try[Boolean] = {
    try {
      Success(fs.delete(new Path(TEMP_DIR), true))
    } catch {
      case ex: Throwable => {
        log.warn(s"Exception when deleting temp files: ${ex.getMessage}", ex)
        Failure(ex)
      }
    }
  }
}
