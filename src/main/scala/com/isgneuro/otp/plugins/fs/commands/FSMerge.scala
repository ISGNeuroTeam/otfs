package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import org.apache.spark.sql.functions._

import java.io.File

class FSMerge(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {
  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  private def createDfWriter: DataFrame => DataFrameWriter[Row] =
    df => df.write.format(format).mode(SaveMode.Append).option("header", "true")

  override def transform(_df: DataFrame): DataFrame = {
    val modelPath = getmodelPath
    val modelExists = new File(modelPath).exists
    if(!modelExists)
      sendError("Model " + model + " doesn't exists.")
    var outBranch = "main"
    val outBranchText = getKeyword("outbranch").getOrElse("main")
    if (outBranchText != "main") {
      val outBranchExists = new File(modelPath + "/" + outBranchText).exists
      outBranch = if (outBranchExists) {
        outBranchText
      } else {
        sendError("Branch " + outBranchText + " doesn't exists in model " + model + ". Use command fsbranch for new branch creation")
      }
    }
    var inBranch = "main"
    val inBranchText = getKeyword("inbranch").getOrElse("main")
    if (inBranchText != "main") {
      val inBranchExists = new File(modelPath + "/" + inBranchText).exists
      inBranch = if (inBranchExists) {
        inBranchText
      } else {
        sendError("Branch " + inBranchText + " doesn't exists in model " + model + ". Use command fsbranch for new branch creation")
      }
    }
    if (outBranch == inBranch) {
      sendError("Out and in branches are equal. Execution of operation is unreachable.")
    }
    val commonReader = spark.read
      .format(format)
      .option("header", "true")
    val dfReader = if (format == "csv") commonReader.option("inferSchema", isInferSchema) else commonReader
    val outBranchConfig = new BranchConfig
    val outLastVersion = outBranchConfig.getLastVersion(modelPath, outBranch).getOrElse("1")
    val outVersion = getKeyword("outbranchversion").getOrElse(outLastVersion)
    val outDataPath = modelPath + "/" + outBranch + "/" + outVersion
    val mergingData: DataFrame = dfReader.load(outDataPath)
    val inBranchConfig = new BranchConfig
    val inLastVersion = inBranchConfig.getLastVersion(modelPath, inBranch).getOrElse("1")
    val inVersion = getKeyword("inbranchversion").getOrElse(inLastVersion)
    val inDataPath =  modelPath + "/" + inBranch + "/" + inVersion
    val dfWriter = createDfWriter(mergingData)
    dfWriter.save(inDataPath)
    mergingData.withColumn("outBranch", format_string(outBranch)).withColumn("outbranchversion", format_string(outVersion))
      .withColumn("inbranch", format_string(inBranch)).withColumn("inbranchversion", format_string(inVersion))
  }
}
