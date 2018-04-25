package com.github.xhanshawn.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("CUR Reader")
      .getOrCreate()
  }
}
