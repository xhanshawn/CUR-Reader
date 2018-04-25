package com.github.xhanshawn.reader

import com.github.xhanshawn.utils.PathUtils
import com.github.xhanshawn.utils.PathUtils.CURPath
import org.scalatest.FunSuite

class CURReader extends FunSuite {
  test("s3 CUR root path") {
    val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/"""
    assert(PathUtils.parseCURPath(path) ==
           Some(CURPath("s3", "20180401-20180501", "0d612072-8651-46db-a8ea-6ec42bd7fa1e", None)))
  }

  test("s3 CUR root path without trailing slash") {
    val path = """s3://cur_bucket/this/is/a/very/long/cur/path/20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e"""
    assert(PathUtils.parseCURPath(path) ==
           Some(CURPath("s3", "20180401-20180501", "3e645c04-9486-42aa-b76f-74350cd4728c", None)))
  }
  test("s3 CUR manifest path") {
    val path = """s3://cur_bucket/this/is/a/very//20180201-20180301/0d612072-8651-46db-a8ea-6ec42bd7fa1e/my-CurReport.json"""
    assert(PathUtils.parseCURPath(path) ==
           Some(CURPath("s3", "20180201-20180301", "3e645c04-9486-42aa-b76f-74350cd4728c", Some("my-CurReport.json"))))
  }
  test("CUR manifest file path") {
    val path = """file://cur_bucket/this/is/a/very//20180401-20180501/0d612072-8651-46db-a8ea-6ec42bd7fa1e/my-CurReport.json"""
    assert(PathUtils.parseCURPath(path) ==
           Some(CURPath("file", "20180401-20180501", "3e645c04-9486-42aa-b76f-74350cd4728c", Some("my-CurReport.json"))))
  }
}
