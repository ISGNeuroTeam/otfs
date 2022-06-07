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

  private var branch = ""

  override def transform(_df: DataFrame): DataFrame = {
    val showDataExistsInfo = getLogicParamValue("showdataexistsinfo")
    val showCreationDate = getLogicParamValue("showcreationdate")
    val showLastUpdateDate = getLogicParamValue("showlastupdatedate")
    val showLastVersionNum = getLogicParamValue("showlastversionnum")
    val showVersionsList = getLogicParamValue("showversionslist")
    val modelPath = getmodelPath
    val modelExists = new File(modelPath).exists
    if(!modelExists)
      sendError("Model " + model + " doesn't exists.")
    val branchText = getKeyword("branch").getOrElse("main")
    if (branchText != "main") {
      val branchExists = new File(modelPath + "/" + branchText).exists
      branch = if (branchExists) {
        branchText
      } else {
        sendError("Branch " + branchText + " doesn't exists in model " + model + ". Use command fsbranch for new branch creation")
      }
    } else {
      sendError("Main branch hasn't parent branches.")
    }
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.exists()) {
      if (branchDirFile.isDirectory) {
        val config = new BranchConfig
        val parentBranchName = config.getParentBranch(modelPath, branch).getOrElse("")
        val parentBranch = Seq(Branch(parentBranchName, config.getParentBranch(modelPath, parentBranchName).getOrElse("")))
        import spark.implicits._
        var parentBranchDf = parentBranch.toDF()
        if (is(showVersionsList)) {
          val versionsListUdf = udf((name: String) => {config.getVersionsList(modelPath, name).getOrElse(Array[String]()).mkString(",")})
          parentBranchDf = parentBranchDf.withColumn("versions", versionsListUdf(col("name")))
        }
        if (is(showLastVersionNum)) {
          val lastVersionUdf = udf((lvName: String) => {config.getLastVersion(modelPath, lvName).getOrElse("")})
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
