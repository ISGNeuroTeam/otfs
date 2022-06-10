package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.StructureInformer
import com.isgneuro.otp.plugins.fs.model.Branch
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths}

class FSGetParentBranch(sq: SimpleQuery, utils: PluginUtils) extends StructureInformer(sq, utils) with OTLSparkSession{

  override def transform(_df: DataFrame): DataFrame = {
    showDataExistsInfo = getLogicParamValue("showdataexistsinfo")
    showCreationDate = getLogicParamValue("showcreationdate")
    showLastUpdateDate = getLogicParamValue("showlastupdatedate")
    showLastVersionNum = getLogicParamValue("showlastversionnum")
    showVersionsList = getLogicParamValue("showversionslist")
    checkModelExisting
    val branch = extractBranchName("branch")
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.exists()) {
      if (branchDirFile.isDirectory) {
        val config = new BranchConfig(modelPath, branch)
        val parentBranchName = config.getParentBranch().getOrElse("")
        val parentConfig = new BranchConfig(modelPath, parentBranchName)
        val parentBranch = Seq(Branch(parentBranchName, parentConfig.getParentBranch().getOrElse("")))
        import spark.implicits._
        var parentBranchDf = parentBranch.toDF()
        if (is(showVersionsList)) {
          val versionsListUdf = udf((name: String) => {parentConfig.getVersionsList().getOrElse(Array[String]()).mkString(",")})
          parentBranchDf = parentBranchDf.withColumn("versions", versionsListUdf(col("name")))
        }
        if (is(showLastVersionNum)) {
          val lastVersionUdf = udf((lvName: String) => {parentConfig.getLastVersion().getOrElse("")})
          parentBranchDf = parentBranchDf.withColumn("lastVersion", lastVersionUdf(col("name")))
        }
        if (is(showDataExistsInfo)) {
          val isDataExistsUdf = udf((deName: String) => {getIsDataContainsText(modelPath, deName)})
          parentBranchDf = parentBranchDf.withColumn("isDataExists", isDataExistsUdf(col("name")))
        }
        if (is(showCreationDate)) {
          val creationDateUdf = udf((cdName: String) => {Files.readAttributes(Paths.get(modelPath + "/" + cdName), classOf[BasicFileAttributes]).creationTime().toString})
          parentBranchDf = parentBranchDf.withColumn("creationDate", creationDateUdf(col("name")))
        }
        if (is(showLastUpdateDate)) {
          val lastUpdateDateUdf = udf((udName: String) => {Files.getLastModifiedTime(Paths.get(modelPath + "/" + udName)).toString})
          parentBranchDf = parentBranchDf.withColumn("lastUpdateDate", lastUpdateDateUdf(col("name")))
        }
        parentBranchDf
      } else {
        sendError("Error: path to branch \" + branch + \" isn't directory.")
      }
    } else {
      sendError("Directory of branch \" + branch + \" isn't exists.")
    }
  }
}
