package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

class FSMerge(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  private val branchConfig = new BranchConfig

  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  private def createDfWriter: DataFrame => DataFrameWriter[Row] =
    df => df.write.format(format).mode(SaveMode.Append).option("header", "true")

  override def transform(_df: DataFrame): DataFrame = {
    checkModelExisting
    val outBranch = extractBranchName("outbranch")
    val inBranch = extractBranchName("inbranch")
    if (outBranch == inBranch) {
      sendError("Out and in branches are equal. Execution of operation is unreachable.")
    }
    val commonReader = spark.read
      .format(format)
      .option("header", "true")
    val dfReader = if (format == "csv") commonReader.option("inferSchema", isInferSchema) else commonReader
    val outVersion = getBranchVersion(outBranch, "outbranchversion")
    val outDataPath = getDataPath(outBranch, outVersion)
    val mergingData: DataFrame = dfReader.load(outDataPath)
    val inVersion = getBranchVersion(inBranch, "inbranchversion")
    val inDataPath =  getDataPath(inBranch, inVersion)
    val dfWriter = createDfWriter(mergingData)
    dfWriter.save(inDataPath)
    mergingData.withColumn("outBranch", format_string(outBranch)).withColumn("outbranchversion", format_string(outVersion))
      .withColumn("inbranch", format_string(inBranch)).withColumn("inbranchversion", format_string(inVersion))
  }

  private def getBranchVersion(branch: String, branchVersionKeyword: String): String = {
    val lastVersion = branchConfig.getLastVersion(modelPath, branch).getOrElse("1")
    getKeyword(branchVersionKeyword).getOrElse(lastVersion)
  }

  private def getDataPath(branch: String, branchVersion: String): String = {
    modelPath + "/" + branch + "/" + branchVersion
  }
}
