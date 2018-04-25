package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.PathUtils
import com.github.xhanshawn.utils.PathUtils.CURPath
import org.apache.spark.sql.{DataFrame, SparkSession}

object CURReader {

  def read(spark: SparkSession, path: String): DataFrame = {

    PathUtils.parseCURPath(path) match {
      case Some(curPath) => {
        spark.read
          .option("headers", true)
          .option("inferSchema", true)
          .csv(path)
      }
      case _ => {
        throw IllegalArgumentException
      }
    }
  }
}
