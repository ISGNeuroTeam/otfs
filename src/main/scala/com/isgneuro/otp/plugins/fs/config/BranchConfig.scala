package com.isgneuro.otp.plugins.fs.config

import com.isgneuro.otp.plugins.fs.config.internals.AnyBranchConfig

import java.io.File

class BranchConfig extends FSConfig {
  val allowedProps: Array[String] = Array("parentBranch", "generatingVersion") ++ AnyBranchConfig.allowedProps

  def create(modelPath: String, branch: String) = {
    val configFile = new File(modelPath + "/" + branch + "/branch.conf")
    createFile(configFile)
  }
}
