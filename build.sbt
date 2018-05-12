name := "cur-reader"
spName := "cur-reader"

version := "0.1"

scalaVersion := "2.11.12"

// set up spark
sparkVersion := "2.2.0"

//val sparkDependencyScope = "provided"

sparkComponents ++= Seq("streaming", "sql")

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.10.77"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"