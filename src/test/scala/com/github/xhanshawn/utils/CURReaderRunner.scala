package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURReader, devConfig}


object CURReaderRunner extends S3Utils {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val spark = sparkSessionBuilder.build()
    CURReader.config = devConfig
    val curs = CURReader.read(spark, path)
    println()
  }
}
