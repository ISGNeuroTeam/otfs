package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.internals.StructureInformer
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSGetBranches(sq: SimpleQuery, utils: PluginUtils) extends StructureInformer(sq, utils) with OTLSparkSession {

  override def transform(_df: DataFrame): DataFrame = {
    //Optional parameters extracting
    showDataExistsInfo = getLogicParamValue("showdataexistsinfo")//Added optional columns to result def, if need//Added optional columns to result def, if need
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
    val modelDirectory = new File(modelPath)
    //Branches defining
    val branchNames: Array[String] = getBranchNames(modelDirectory.listFiles.filter(_.isDirectory))
    log.debug("Defined branches: " + branchNames.mkString(",") + ".")
    //Create base df without optional columns
    val branches = createBranchesDataframe(branchNames)
    //Added optional columns to result def, if need
    completeBranchesDataframe(branches)
  }
}
