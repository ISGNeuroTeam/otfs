package com.isgneuro.otp.plugins.fs.config

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}

trait FSConfig {
  private var file: File = _

  val allowedProps: Array[String]

  protected def createFile(configFile: File) = {
    if (!configFile.exists)
      configFile.createNewFile()
    file = configFile
  }

  def addContent(text: String) = {
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.write(text)
    writer.write("\r\n")
    writer.close()
  }

  def addEntries(entries: Map[String, String]) = {
    for (e <- entries) {
      addEntry(e._1, e._2)
    }
  }

  def addEntry(key: String, value: String) = {
    if (allowedProps.contains(key)) {
      val writer = new PrintWriter(file)
      writer.write(key + "=" + value + "\r\n")
      writer.close()
    } else {
      throw new IllegalArgumentException("Not allowed config property")
    }
  }

  def addListEntry(key: String, values: Array[String]) = {
    if (allowedProps.contains(key)) {
      val writer = new FileWriter(file)
      writer.write(key + "=[" + values.mkString(",") + "]")
    } else {
      throw new IllegalArgumentException("Not allowed config property")
    }
  }
}
