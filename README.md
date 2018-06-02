# CUR-Reader
A scala/spark package to load CUR files from AWS S3.

## Usage

### Spark Shell
Start spark shell with cur-reader fat jar.
```$scala
scala> spark-shell --jars path/to/cur-reader-0.1.3.jar
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

#### Path format
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

