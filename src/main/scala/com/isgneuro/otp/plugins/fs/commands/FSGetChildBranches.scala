package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.StructureInformer
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSGetChildBranches(sq: SimpleQuery, utils: PluginUtils) extends StructureInformer(sq, utils) with OTLSparkSession{

  override def transform(_df: DataFrame): DataFrame = {
    //Optional parameters extracting
    showDataExistsInfo = getLogicParamValue("showdataexistsinfo")
    showCreationDate = getLogicParamValue("showcreationdate")
    showLastUpdateDate = getLogicParamValue("showlastupdatedate")
    showLastVersionNum = getLogicParamValue("showlastversionnum")
    hasChildBranches = getLogicParamValue("haschildbranches")
    onlyEmpty = getLogicParamValue("onlyempty")
    onlyNonEmpty = getLogicParamValue("onlynonempty")
    if (onlyEmpty == "true" && onlyEmpty == onlyNonEmpty) {
      sendError("Error: equal values in opposite parameters onlyempty and onlynonempty. Define various values in these params or define only one param.")
    }
    onlyWithChildBranches = getLogicParamValue("onlywithchildbranches")
    onlyWithoutChildBranches = getLogicParamValue("onlywithoutchildbranches")
    if (onlyWithChildBranches == "true" && onlyWithChildBranches == onlyWithoutChildBranches) {
      sendError("Error: equal values in opposite parameters onlyWithChildBranches and onlyWithoutChildBranches. Define various values in these params or define only one param.")
    }
    showVersionsList = getLogicParamValue("showversionslist")
    //Model dir defining
    checkModelExisting
    //Branch defining
    val branch = extractBranchName("branch")
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.exists()) {
      if (branchDirFile.isDirectory) {
        val config = new BranchConfig(modelPath, branch)
        //All existing child branches for branch defining
        val allChildBranchNames = config.getChildBranches().getOrElse(Array[String]())
        val modelDirectory = new File(modelPath)
        //Filter branches by conditions, defining in optional params
        val childBranchNames: Array[String] = getBranchNames(modelDirectory.listFiles.filter(f => f.isDirectory && allChildBranchNames.contains(f.getName)))
        log.debug("Defined branches: " + childBranchNames.mkString(",") + ".")
        //Create base df without optional columns
        val childBranches = createBranchesDataframe(childBranchNames)
        //Added optional columns to result def, if need
        completeBranchesDataframe(childBranches)
      } else {
        sendError("Error: path to branch \" + branch + \" isn't directory.")
      }
    } else {
      sendError("Directory of branch \" + branch + \" isn't exists.")
    }
  }
}
