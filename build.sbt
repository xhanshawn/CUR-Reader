import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

name := "cur-reader"
spName := "cur-reader"

version := "0.2.0"

scalaVersion := "2.11.12"

// set up spark
sparkVersion := "2.2.0"

// add a JVM option to use when forking a JVM for 'run'
javaOptions ++= Seq("-Xmx2G", "-Xms4G")

spIgnoreProvided := true

sparkComponents ++= Seq("streaming", "sql")

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.82"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

// Need to shade duplicate name files to assembly a fat jar.
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.amazonaws.**" -> "shadedstuff.awscore.@1").inLibrary("com.amazonaws" % "aws-java-sdk" % "1.7.4"),
  ShadeRule.rename("org.apache.commons.beanutils.**" -> "shadedstuff.beanutils1_7.@1").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
  ShadeRule.rename("org.apache.commons.collections.**" -> "shadedstuff.beanutils1_7.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
  ShadeRule.rename("org.apache.commons.beanutils.**" -> "shadedstuff.beanutils1-8.@1").inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0"),
  ShadeRule.rename("org.apache.commons.collections.**" -> "shadedstuff.beanutils1_8.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0")
)

// Need to resource files to avoid deduplicate error.
assemblyMergeStrategy in assembly := {
  case "mime.types" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

initialCommands in console :=
  s"""
     |import com.github.xhanshawn.utils._
     |import com.github.xhanshawn.reader._
     |val spark = sparkSessionBuilder.build()
     |val sc = spark.sparkContext
     |val sqlContext = spark.sqlContext
     |
   """.stripMargin

cleanupCommands in console :=
  s"""
     |spark.stop()
   """.stripMargin

// Generate a jar with version and good file name format.
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}

assemblyJarName in assembly := s"${name.value}-${version.value}.${artifact.value.extension}"
