package com.isgneuro.otp.plugins.fs.config

import com.isgneuro.otp.plugins.fs.config.internals.AnyBranchConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import java.io.File
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class BranchConfig extends FSConfig {
  val allowedProps: Array[String] = Array("parentBranch", "generatingVersion") ++ AnyBranchConfig.allowedProps

  val allowedStatuses: Array[String] = Array("init", "hasData")

  def create(modelPath: String, branch: String) = {
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    createFile(configFile)
  }

  def getLastVersion(modelPath: String, branch: String): String = {
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    config.getString("lastversion")
  }

  def getStatus(modelPath: String, branch: String): String = {
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    config.getString("status")
  }

  def setStatus(modelPath: String, branch: String, status: String) = {
    if (!allowedStatuses.contains(status))
      throw new Exception("Not valid branch status setting")
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    val statusContent = ConfigFactory.empty.withValue("status", ConfigValueFactory.fromAnyRef(status))
    //?
  }

  def getChildBranches(modelPath: String, branch: String): Array[String] = {
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    val config: Config = ConfigFactory.parseFile(configFile)
    config.getStringList("childbranches").asScala.toArray
  }
}
