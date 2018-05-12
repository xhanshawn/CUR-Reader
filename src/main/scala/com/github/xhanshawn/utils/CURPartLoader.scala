package com.github.xhanshawn.utils

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.github.xhanshawn.reader.CURPart
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object CURPartLoader {
  def load(spark: SparkSession, curParts: Dataset[CURPart]): Unit ={
    val csvDS = spark.createDataset(keys).flatMap { key =>

    }
    spark.read.csv(csvDS)
  }
}
