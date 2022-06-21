package com.isgneuro.otp.plugins.fs.config

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueFactory}

import java.io.{File, FileOutputStream, PrintWriter}
import java.util

trait FSConfig {
  private var file: File = _

  protected def rewriteIterEntries(file: File, name: String, entries: util.List[String]): Some[Boolean] = {
    val content = ConfigFactory.empty.withValue(name, ConfigValueFactory.fromIterable(entries))
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.write(content.root().render(ConfigRenderOptions.concise()))
    writer.write("\r\n")
    writer.close()
    Some(true)
  }


  protected def createConfigExecWork(name: String, configValue: ConfigValue, pathPart: String) = {
    //for refactoring of config classes - delete duplicates of createConfigExec
  }

  protected def resetConfig(name: String, value: String, pathPart: String) = {

  }

  protected def addToListConfig(name: String, values: java.lang.Iterable[String], pathPart: String) = {

  }

}
