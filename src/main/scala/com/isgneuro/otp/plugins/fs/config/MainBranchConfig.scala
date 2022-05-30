package com.isgneuro.otp.plugins.fs.config
import com.isgneuro.otp.plugins.fs.config.internals.AnyBranchConfig

import java.io.File

class MainBranchConfig extends FSConfig {
  val allowedProps: Array[String] = AnyBranchConfig.allowedProps

  def create(modelPath: String) = {
    val configFile = new File(modelPath + "/main/branch.conf")
    createFile(configFile)
  }
}
