package com.github.xhanshawn.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = build()

  def build(): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("CUR Reader")
      .getOrCreate()
  }
}

object sparkSessionBuilder extends SparkSessionWrapper {
  def getOrBuild(): SparkSession = {
    spark
  }
}
