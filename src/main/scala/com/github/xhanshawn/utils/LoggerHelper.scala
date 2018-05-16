package com.github.xhanshawn.utils

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext

trait LoggerHelper {
  var previousLoggerLevel = Level.INFO
  lazy val log = LogManager.getRootLogger

  def configLogger(sc: SparkContext): Unit = {
    previousLoggerLevel = log.getLevel()
    log.setLevel(Level.WARN)
  }

  def resetLogLevel(): Unit = {
    log.setLevel(previousLoggerLevel)
  }
}
