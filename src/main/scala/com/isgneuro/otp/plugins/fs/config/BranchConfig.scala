package com.isgneuro.otp.plugins.fs.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueFactory}

import java.io.{File, FileOutputStream, PrintWriter}
import java.util
import scala.collection.JavaConversions.{asJavaCollection, iterableAsScalaIterable}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class BranchConfig(modelPath: String, branch: String) extends FSConfig {

  val allowedStatuses: Array[String] = Array("init", "hasData")

  def createConfig(name: String, value: String): Option[Boolean] = {
    try {
      createConfigExec(name, ConfigValueFactory.fromAnyRef(value))
      Some(true)
    } catch {
      case e: Exception => None
    }
  }


  def createOrAddToList(name: String, values: java.lang.Iterable[String]): Option[Boolean] = {
    val file = new File(modelPath + "/" + branch + "/" + name + ".conf")
    if (file.exists()) {
      addToListConfig(name, values)
    } else {
      createListConfig(name, values)
    }
  }

  def createListConfig(name: String, values: java.lang.Iterable[String]): Option[Boolean] = {
    try {
      createConfigExec(name, ConfigValueFactory.fromIterable(values))
      Some(true)
    } catch {
      case e: Exception => None
    }
  }

  private def createConfigExec(name: String, configValue: ConfigValue) = {
    val file = new File(modelPath + "/" + branch + "/" + name + ".conf")
    val content = ConfigFactory.empty.withValue(name, configValue)
    val writer = new PrintWriter(new FileOutputStream(file, true))
    writer.write(content.root().render(ConfigRenderOptions.concise()))
    writer.write("\r\n")
    writer.close()
  }

  def resetConfig(name: String, value: String): Option[Boolean] = {
    val file = new File(modelPath + "/" + branch + "/" + name + ".conf")
    if (file.exists) {
      try {
        if(file.delete()) {
          if (file.createNewFile()) {
            val content = ConfigFactory.empty.withValue(name, ConfigValueFactory.fromAnyRef(value))
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

  def addToListConfig(name: String, values: java.lang.Iterable[String]): Option[Boolean] = {
    val file = new File(modelPath + "/" + branch + "/" + name + ".conf")
    if (file.exists) {
      try {
        val config: Config = ConfigFactory.parseFile(file)
        val entries: util.List[String] = config.getStringList(name)
        if(file.delete()) {
          if (file.createNewFile()) {
            entries.addAll(values.toList)
            rewriteIterEntries(file, name, entries)
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
    val file = new File(modelPath + "/" + branch + "/" + name + ".conf")
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

  def getLastVersion(): Option[String] = {
    val file = new File(modelPath + "/" + branch + "/lastversion.conf")
    val config: Config = ConfigFactory.parseFile(file)
    try {
      val result = config.getString("lastversion")
      Some(result)
    } catch {
      case e: Exception => None
    }
  }

  def getStatus(): Option[String] = {
    val configFile = new File(modelPath + "/" + branch + "/status.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    try {
      val result = config.getString("status")
      Some(result)
    } catch {
      case e: Exception => None
    }
  }

  def getParentBranch(): Option[String] = {
    val configFile = new File(modelPath + "/" + branch + "/parentbranch.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    try {
      val result = config.getString("parentbranch")
      Some(result)
    } catch {
      case e: Exception => None
    }
  }

  def getChildBranches(): Option[Array[String]] = {
    val configFile = new File(modelPath + "/" + branch + "/childbranches.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    try {
      val result = config.getStringList("childbranches").asScala.toArray
      Some(result)
    } catch {
      case e: Exception => None
    }
  }

  def getVersionsList(): Option[Array[String]] = {
    val configFile = new File(modelPath + "/" + branch + "/versions.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    try {
      val result = config.getStringList("versions").asScala.toArray
      Some(result)
    } catch {
      case e: Exception => None
    }
  }

}
