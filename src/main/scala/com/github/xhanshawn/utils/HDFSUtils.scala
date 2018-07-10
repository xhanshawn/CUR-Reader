package com.github.xhanshawn.utils

import java.io.{BufferedOutputStream, InputStream, OutputStream}
import java.util.zip.GZIPOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object HDFSUtils extends LoggerHelper with Serializable {
  final private val TempDir = "/tmp/cur-reader/curs/".intern()

  private var sparkConf: SparkConf = null

  def setHDFSConfig(spark: SparkSession): Unit = {
    if (sparkConf == null) {
      sparkConf = spark.sparkContext.getConf
    } else {
      log.debug("HDFS is already set up.")
    }
  }

  def createTempFile(filePath: String): OutputStream = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val fileSys = FileSystem.get(hadoopConf)
    new BufferedOutputStream(new GZIPOutputStream(fileSys.create(getTempFilePath(filePath))))
  }

  def getTempFilePath(fileName: String): Path = {
    new Path(TempDir, fileName)
  }

  def fileExists(filePath: String): Boolean = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val fileSys = FileSystem.get(hadoopConf)
    fileSys.exists(getTempFilePath(filePath))
  }

  def copyToFile[O <: OutputStream](inputStream: InputStream, outputStream: O): Unit = {
    val buffer = new Array[Byte](65536)
    Stream.continually(inputStream.read(buffer)).takeWhile(_ != -1).foreach(outputStream.write(buffer, 0, _))
    inputStream.close()
    outputStream.close()
  }

  def deleteAllTempFiles(): Try[Boolean] = {
    clearTempFile(TempDir)
  }

  def clearTempFiles(spark: SparkSession = null): Unit = {
    HDFSUtils.setHDFSConfig(spark)
    HDFSUtils.deleteAllTempFiles() match {
      case Success(_) => log.warn("Temp files are deleted.")
      case Failure(ex) => throw ex
    }
  }

  def clearTempFile(file: String, spark: SparkSession = null): Try[Boolean] = {
    if (spark == null) {
      if (sparkConf == null) throw new RuntimeException("HDFS Config is not set. File is not going to be cleaned.")
    } else {
      setHDFSConfig(spark)
    }

    try {
      val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
      val fileSys = FileSystem.get(hadoopConf)
      Success(fileSys.delete(new Path(file), true))
    } catch {
      case ex: Throwable => {
        log.warn(s"Exception when deleting temp files: ${ex.getMessage}", ex)
        Failure(ex)
      }
    }
  }
}
