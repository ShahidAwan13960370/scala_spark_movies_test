package com.test.movies
import java.io.File.{separator}

object Util {
  def getFilePath(dir: String, file: String): String ={
    dir.concat(separator).concat(file)
  }
}
