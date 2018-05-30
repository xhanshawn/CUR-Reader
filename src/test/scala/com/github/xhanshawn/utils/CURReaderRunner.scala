package com.github.xhanshawn.utils

import java.net.URI

import com.github.xhanshawn.reader.{CURReader, devConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

object CURReaderRunner extends S3Utils {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val spark = sparkSessionBuilder.build()
    CURReader.config = devConfig
    val curs = CURReader.read(spark, path)
    println()
  }
}
