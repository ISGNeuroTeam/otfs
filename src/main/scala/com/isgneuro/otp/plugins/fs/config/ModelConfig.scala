package com.isgneuro.otp.plugins.fs.config

import java.io.File

class ModelConfig extends FSConfig {
  val allowedProps: Array[String] = Array("modelname", "pathbranch_")

  def create(modelPath: String) =  {
    val configFile = new File(modelPath + "model.conf")
    createFile(configFile)
  }
}
