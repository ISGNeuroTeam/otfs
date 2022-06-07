package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import java.io.File

class FSGet(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  private var branch = "main"

  override def transform(_df: DataFrame): DataFrame = {
    val modelPath = getmodelPath
    val modelExists = new File(modelPath).exists
    if(!modelExists)
      sendError("Model " + model + " doesn't exists.")
    val commonReader = spark.read
      .format(format)
      .option("header", "true")
    val dfReader = if (format == "csv") commonReader.option("inferSchema", isInferSchema) else commonReader
    val branchText = getKeyword("branch").getOrElse("main")
    if (branchText != "main") {
      val branchExists = new File(modelPath + "/" + branchText).exists
      branch = if (branchExists) {
        branchText
      } else {
        sendError("Branch " + branchText + " doesn't exists in model " + model + ". Use command fsbranch for new branch creation")
      }
    }
    val branchConfig = new BranchConfig
    val lastVersion = branchConfig.getLastVersion(modelPath, branch).getOrElse("1")
    val version = getKeyword("version").getOrElse(lastVersion)
    val dataPath = modelPath + "/" + branch + "/" + version
    dfReader.load(dataPath)
  }
}