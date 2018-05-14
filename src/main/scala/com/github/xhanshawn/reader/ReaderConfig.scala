package com.github.xhanshawn.reader

class ReaderConfig {
  var usingAWSAPI: Boolean = false
  var readFull: Boolean = true
  var inferSchema: Boolean = false
}

case object runningConfig extends ReaderConfig

case object defaultConfig extends ReaderConfig
case object devConfig extends ReaderConfig {
  usingAWSAPI = true
  readFull = false
}


