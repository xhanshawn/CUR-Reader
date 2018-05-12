package com.github.xhanshawn.utils

import com.github.xhanshawn.utils.PathUtils.CURPath
import org.scalatest.FunSpec

class PathUtilsSpec extends FunSpec {
  describe("CURPath") {
    it("should create from s3 CUR root path") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/long/cur/path", "20180401-20180501", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", null))
    }

    it("should create from s3 CUR root path without trailing slash") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/long/cur/path",  "20180401-20180501", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", null))
    }
    it("should create from s3 CUR manifest path") {
      val path = """s3://cur_bucket/this/is/a/very//20180201-20180301/0d612072-8651-46db-a8ea-6ec42bd7fa1e/my-CurReport-Manifest.json"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3", "cur_bucket/this/is/a/very/",  "20180201-20180301", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", "my-CurReport"))
    }
    it("should create from CUR manifest file path") {
      val path = """file://cur_bucket/short//20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/my-CurReport-Manifest.json"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("file", "cur_bucket/short/",  "20180401-20180501", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", "my-CurReport"))
    }

    it("should create from s3n CUR manifest regular path") {
      val path = """s3n://cur_bucket/this/is/a/very//20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/my-CurReport-part-6.csv.gz"""
      assert(PathUtils.parseCURPath(path) ==
        CURPath("s3n", "cur_bucket/this/is/a/very/",  "20180401-20180501", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", null))
    }

    describe("rootPath") {
      val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/"""
      val curPath = PathUtils.parseCURPath(path)
      assert(curPath.rootPath ==
        "s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e")
    }
  }
}
