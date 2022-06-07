package com.isgneuro.otp.plugins.fs.internals

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class StructureInformer(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils){

  private val allowedLogicParamValues = Array("true", "false")

  protected def getLogicParamValue(name: String): String = {
    val result = getKeyword(name).getOrElse("false")
    if (!allowedLogicParamValues.contains(result))
      sendError("Not allowed value for parameter " + name)
    result
  }

  protected def is(logicText: String): Boolean = {
    logicText match {
      case "true" => true
      case _ => false
    }
  }

  protected def getIsChildBranchesExistsText(modelPath: String, branch: String): String = {
    if (childBranchesExistsIn(modelPath, branch)) "y" else "n"
  }

  protected def childBranchesExistsIn(modelPath: String, branch: String): Boolean = {
    val branchConfig = new BranchConfig
    branchConfig.getChildBranches(modelPath, branch).getOrElse(Array[String]()).length > 0
  }

  protected def getIsDataContainsText(modelPath: String, branch: String): String = {
    if (dataContainsIn(new File(modelPath + "/" + branch))) "y" else "n"
  }

  protected def dataContainsIn(dir: File): Boolean = {
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isDirectory).forall(_.listFiles.length > 0)
    } else {
      sendError("Checking file on directory properties.")
    }
  }
}
