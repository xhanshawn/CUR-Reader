package com.github.xhanshawn.utils

import java.io._

import org.apache.commons.io.IOUtils
import org.scalatest.FunSpec

class HDFSUtilsSpec extends FunSpec {
  describe("copyToFile") {
    it("should copy simple string stream") {
      val inputStr = Array.fill[String](100000)("InputSring\n").mkString
      println(inputStr.getBytes.length)
      val input = new ByteArrayInputStream(inputStr.getBytes)
      val output = new ByteArrayOutputStream()
      HDFSUtils.copyToFile(input, output)
      val outputStr = new String(output.toByteArray)
      assert(outputStr === inputStr)
    }

    it("should copy .csv.gz files") {
      val inputStream = getClass.getResourceAsStream("/testFile.csv.gz")
      val input = new BufferedInputStream(inputStream)
      val output = new ByteArrayOutputStream()
      val bufferedOutput = new BufferedOutputStream(output)
      HDFSUtils.copyToFile(input, bufferedOutput)
      val inputStr = new String(IOUtils.toByteArray(getClass.getResourceAsStream("/testFile.csv.gz")))
      val outputStr = new String(output.toByteArray)
      assert(outputStr === inputStr)
    }
  }
}
