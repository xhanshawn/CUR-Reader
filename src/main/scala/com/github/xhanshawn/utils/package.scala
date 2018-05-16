package com.github.xhanshawn

import java.io.File

package object utils {

  def getFileName(path: String): String = {
    new File(path).getName()
  }

  def ensureS3PathToFilePath(key: String): String = {
    key.split("/").map(e => if (e == "") "_" else e).mkString("/")
  }
}
