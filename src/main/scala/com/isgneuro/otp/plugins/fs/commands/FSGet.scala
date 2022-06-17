package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

class FSGet(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  override def transform(_df: DataFrame): DataFrame = {
    checkModelExisting
    val commonReader = spark.read
      .format(format)
      .option("header", "true")
    val dfReader = if (format == "csv") commonReader.option("inferSchema", isInferSchema) else commonReader
    val branch = extractBranchName("branch")
    val branchConfig = new BranchConfig(modelPath, branch)
    val lastVersion = branchConfig.getLastVersion().getOrElse("1")
    val version = getKeyword("version").getOrElse(lastVersion)
    val dataPath = modelPath + "/" + branch + "/" + version
    try {
      val result = dfReader.load(dataPath)
      log.info("Data loaded from model " + model + ", branch " + branch + ", version " + version)
      result
    } catch {
      case e: Exception => sendError("Branch " + branch + ", version " + version + " doesn't exists or doesn't contains data")
    }
  }
}