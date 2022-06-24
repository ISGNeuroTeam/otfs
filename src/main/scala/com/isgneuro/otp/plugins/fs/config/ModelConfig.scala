package com.isgneuro.otp.plugins.fs.config

import com.google.gson.JsonObject
import com.google.gson.stream.JsonWriter
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueFactory}

import scala.collection.JavaConversions.{asJavaCollection, iterableAsScalaIterable}
import java.io.{File, FileOutputStream, FileWriter, PrintWriter}
import java.util

class ModelConfig(modelPath: String) extends FSConfig {

  def createConfig(name: String, value: String) = {
    createConfigExec(name, ConfigValueFactory.fromAnyRef(value))
  }

  def createListConfig(name: String, values: java.lang.Iterable[String]) = {
    createConfigExec(name, ConfigValueFactory.fromIterable(values))
  }

  private def createConfigExec(name: String, configValue: ConfigValue) = {
    val file = new File(modelPath + "/" + name + ".conf")
    val content = ConfigFactory.empty.withValue(name, configValue)
    /*val jwriter = new JsonWriter(new FileWriter(file, true))
    val jsonObject = new JsonObject
    jsonObject.add(name, )*/
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.write(content.root().render(ConfigRenderOptions.concise()))
    writer.write("\r\n")
    writer.close()
  }

  def addToListConfig(name: String, values: java.lang.Iterable[String]): Option[Boolean] = {
    val file = new File(modelPath + "/" + name + ".conf")
    if (file.exists) {
      try {
        val config: Config = ConfigFactory.parseFile(file)
        val entries = config.getStringList(name)
        if(file.delete()) {
          if (file.createNewFile()) {
            entries.addAll(values.toList)
            val content = ConfigFactory.empty.withValue(name, ConfigValueFactory.fromIterable(entries))
            val writer = new PrintWriter(new FileOutputStream(file, true))
            writer.write(content.root().render(ConfigRenderOptions.concise()))
            writer.write("\r\n")
            writer.close()
            Some(true)
          } else {
            None
          }
        } else {
          None
        }
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }

  def removeFromListConfig(name: String, values: java.lang.Iterable[String]): Option[Boolean] = {
    val file = new File(modelPath + "/" + name + ".conf")
    if (file.exists) {
      try {
        val config: Config = ConfigFactory.parseFile(file)
        val entries: util.List[String] = config.getStringList(name)
        for(value <- values) {
          entries.remove(value)
        }
        if (entries.size() == 0) {
          file.delete()
          Some(true)
        } else {
          if (file.delete()) {
            if (file.createNewFile()) {
              rewriteIterEntries(file, name, entries)
            } else {
              None
            }
          } else {
            None
          }
        }
      } catch {
        case e: Exception => None
      }
    } else {
      None
    }
  }

}
