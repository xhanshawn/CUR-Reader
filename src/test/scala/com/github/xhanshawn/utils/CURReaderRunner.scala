package com.github.xhanshawn.utils

import com.github.xhanshawn.reader.{CURReader, devConfig}

object CURReaderRunner {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val spark = sparkSessionBuilder.build()
//    val curs = CURReader.read(spark, List(path))
    CURReader.config = devConfig
    val curs = CURReader.read(spark, List(path))
    println()
  }
}
