# CUR-Reader
A scala/spark package to load CUR files from AWS S3.

## Usage

### Quick Start
#### Spark Shell
Start spark shell with cur-reader fat jar.
```$scala
scala> spark-shell --jars path/to/cur-reader-<version>.jar
```
Load a CUR from S3.

```$scala
// import the package of the reader.
scala> import com.github.xhanshawn.reader._
 
// utils package makes you access reader configurations and some helpers. 
scala> import com.github.xhanshawn.utils._
scala> val path = "s3n://bucket/path/to/you/cur/20180201-20180301/"
 
// This is an object of class CUR consist of information for CUR manifest and CUR rows.
scala> val cur = CURReader.read(spark, path)
scala> val rows = cur.curRows // Dataframe for the CUR rows.
scala> rows.printSchema()
```
#### Include as Dependency
##### Publish Locally
One way to include CUR-Reader as your dependency is to publish it locally and then assembly it to the fat jar
of your project.
```
$ sbt compile
$ sbt package
$ sbt publishLocal
[info] Loading global plugins from /Users/foo/.sbt/0.13/plugins
[info] Loading project definition from /Users/foo/workspace/cur-reader/project
[info] Set current project to cur-reader (in build file:/Users/foo/workspace/cur-reader/)
[info] Wrote /Users/foo/workspace/cur-reader/target/scala-2.11/cur-reader_2.11-2.2.0_0.2.0.pom
[info] :: delivering :: com.github.xhanshawn#cur-reader_2.11;0.2.0 :: 0.2.0 :: release :: Wed Jul 25 21:20:47 UTC 2018
[info]      delivering ivy file to /Users/foo/workspace/cur-reader/target/scala-2.11/ivy-0.2.0.xml
[info]      published cur-reader_2.11 to /Users/foo/.ivy2/local/com.github.xhanshawn/cur-reader_2.11/0.2.0/poms/cur-reader_2.11.pom
[info]      published cur-reader_2.11 to /Users/foo/.ivy2/local/com.github.xhanshawn/cur-reader_2.11/0.2.0/jars/cur-reader_2.11.jar
[info]      published cur-reader_2.11 to /Users/foo/.ivy2/local/com.github.xhanshawn/cur-reader_2.11/0.2.0/srcs/cur-reader_2.11-sources.jar
[info]      published cur-reader_2.11 to /Users/foo/.ivy2/local/com.github.xhanshawn/cur-reader_2.11/0.2.0/docs/cur-reader_2.11-javadoc.jar
[info]      published ivy to /Users/foo/.ivy2/local/com.github.xhanshawn/cur-reader_2.11/0.2.0/ivys/ivy.xml
```
Then you need to include it `build.sbt` like
```
resolvers += Resolver.file("cur-reader", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)

libraryDependencies += "com.github.xhanshawn" %% "cur-reader" % "0.2.0"
```

The source code can be imported in your projected.

### Query Utils

#### Query Helpers
There are some query utilities available in the CUR Reader. They are very helpful to analyze CUR data
and providing quick ways to query by services, reservation types, line items type, etc.

```$scala
scala> val cur = CURReader.read(spark, path)
 
// Query EC2 RI Summary Rows.
scala> cur.ec2.ri.summary
res1: com.github.xhanshawn.reader.CUR = CUR(CURPath(s3,bucket/path/to/you/cur/20180201-20180301/,null,report),CURManifest(84196c18-af8e-4378-800f-697425c0dae4,2b4047e8f325c3bfa5b49a78b36c281da8b9a8161af07f99012850b62e948517,833008473584,BillingPeriod(2018-07-01 00:00:00.0,2018-08-01 00:00:00.0),[Lcom.github.xhanshawn.reader.CURColumn;@77f4742b,report,[Ljava.lang.String;@39b3de87,FormatData(UTF-8,GZIP,text/csv)),[identity/LineItemId: string, identity/TimeInterval: string ... 161 more fields])
 
// To get the DataFrame
scala> cur.ec2.ri.summary.curRows
res2: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [identity/LineItemId: string, identity/TimeInterval: string ... 161 more fields]
 
// CUR class also supports some basic clauses like where, select, withColumn.
scala> cur.ec2.ri.where("column1 = val1")
```

The basic idea here it to predefine helpers that can be chained on CUR class. More details can be found in com.xhanshawn.reader.CURQUeryUtils

#### Print Rows

To visualize query results, of course you can use `show` to print out Dataset results. But it doesn't look very nice with 100 columns.
So there is a helper called `printRows()` which will print vertically for results with more than 10 columns. It looks better with just a few rows in the result.

```$scala
scala> cur.ec2.ri.summary.printRows()
```

#### Write Results

To write query results, still, you can always to use default spark writer after you get the DataFrame of CUR rows. But sometimes,
when you want to export results into single file, it runs very slowly if you just do `df.repartition(1).write.csv("file")`. It
often happens when you do a complex query with many partitions, because repartitioning data into 1 needs a lot of data shuffling.
One way I like to deal with cases like this it to save the results temporarily into multiple parts and read the temp results back.
Then we do repartitioning and write the results into 1 file. It is faster because the query results usually are much smaller than
the original rows.

CUR-Reader provides a utility to help you do this trick. `write` helper of the CUR class accepts partition num parameter. If it is
1, the helper will do this trick for you.

```$scala
//Write all the EC2 RI summary lines into one single CSV file with headers.
scala> cur.ec2.ri.summary.write.option("header", true).csv("file")
``` 

`write` helper returns `DataFrameWriter` which is same as the return type of `DataFrame.write`.
`write` helper implicitly get or build spark session because it needs to read files.

### Path format
You can choose to give the reader a very specific CUR path. If you give it a directory of multiple CURs, it wlll try to
find the lastest or ROOT curs. The following are acceptable CUR paths:

```
"s3n://bucket/path/to/you/cur/20180201-20180301/"
 
"s3n://bucket/path/to/you/cur/20180201-20180301/abcd-assembly-id/"
 
"s3n://bucket/path/to/you/cur/20180201-20180301/abcd-assembly-id/report-Manifest.json"
 
"s3a://bucket/path/to/you/cur/20180201-20180301/abcd-assembly-id/report-Manifest.json"
 
"s3://bucket/path/to/you/cur/20180201-20180301/abcd-assembly-id/report-Manifest.json"
 
"s3://bucket/path/to//you/cur/20180201-20180301/abcd-assembly-id/report-Manifest.json"
```
### Configration

#### How to load
There are two ways to load a CUR in the reader. One is to use hadoop-aws to directly loading files from s3. It doesn't 
have much difference from loading by `spark.read.csv("")`. But the reader will validate the path you are using and help
you find CURs under the directory you pass in.

The other way is to use AWS Java SDK to temporarily download CURs. The motivation for this is that the old version hadoop
cannot load from s3 path with double slashes like `s3://bucket/path//to/cur/20180101-20180201/`. This downloading solution
 is for cases like this. Downloading does take more time and it has a disk space issue which means you need to provide dfs
 space more than the size of the entire CUR.
#### Pre-defined configurations.
Default configuration will make reader directly use s3 URLs by default and read full of CUR.
##### Cache Config
This configuration will make the reader download CURs.
```$scala
CURReader.config = cacheConfig
```
##### Development Config
This configuration will make the reader read only one part of CURs and download it.
```$scala
CURReader.config = devConfig
```

