package com.github.xhanshawn.utils

import com.amazonaws.services.s3.model.S3ObjectSummary
import com.github.xhanshawn.reader.CURPath
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterEach, FlatSpec, FunSpec}

class ParceCURPathSpec extends FunSpec {
  describe("CURPath") {
    it("should create from s3 CUR root path") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/long/cur/path", "20180401-20180501", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", null))
    }

    it("should create from s3 CUR root path without trailing slash") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/long/cur/path",  "20180401-20180501", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", null))
    }
    it("should create from s3 CUR manifest path") {
      val path = """s3://cur_bucket/this/is/a/very//20180201-20180301/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/my-CurReport-Manifest.json"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/",  "20180201-20180301", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", "my-CurReport"))
    }
    it("should create from CUR manifest file path") {
      val path = """file://cur_bucket/short//20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/my-CurReport-Manifest.json"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("file", "cur_bucket/short/",  "20180401-20180501", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", "my-CurReport"))
    }

    it("should create from s3n CUR manifest regular path") {
      val path = """s3n://cur_bucket/this/is/a/very//20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/my-CurReport-part-6.csv.gz"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3n", "cur_bucket/this/is/a/very/",  "20180401-20180501", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", null))
    }

    describe("rootPath") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/"""
      val curPath = PathUtils.parseCURPath(path)
      assert(curPath.curDirectory ==
        "s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e")
    }
  }
}

class FindCURManifestSpec extends FunSpec {
  describe("findCURManifest") {
    it("should find CURManifest") {
      val keys = List(
        "/path/to/CUR/20180101-20180201/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/report-Manifest.json",
        "/path/to/CUR/20171201-20180101/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/report-Manifest.json",
        "/path/to/CUR/20180101-20180201/207393df-d3bd-46db-a8ea-6ec42bd7fa1e/report-Manifest.json",
        "/path/to/CUR/20180101-20180201/report-Manifest.json",
        "/path/to/CUR/20180101-20180201/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/report-part-1.csv.gz",
        "/path/to/CUR/20170801-20170901/0d612072-d3bd-46db-a8ea-6ec42bd7fa1e/report-Manifest.json"
      )
      val curPath = CURPath("s3", "bucket/path/to/CUR", "20180101-20180201", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", null)
      val truthPath = CURPath("s3", "bucket/path/to/CUR", "20180101-20180201", "0d612072-d3bd-46db-a8ea-6ec42bd7fa1e", "report")
      assert(PathUtils.findManifestFromPaths(keys, curPath) === List(truthPath))
    }
  }
}
