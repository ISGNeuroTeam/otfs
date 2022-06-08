package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSBranch (sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession{

  override def transform(_df: DataFrame): DataFrame = {
    checkModelExisting
    val branch = getKeyword("name") match {
      case Some(name) => name
      case None => sendError("branch name is not specified")
    }
    val fromBranch = extractBranchName("from")
    val branchPath = modelPath + "/" + branch
    val branchDir = new File(branchPath)
    val branchCreateSucc = branchDir.mkdirs()
    if (branchCreateSucc) {
      val version1Path = branchPath + "/1"
      val version1Dir = new File(version1Path)
      val version1CreateSucc = version1Dir.mkdirs()
      if (version1CreateSucc){
        val branchConfig = new BranchConfig
        branchConfig.create(modelPath, branch)
        val branchCfgContent = ConfigFactory.empty.withValue("branchname", ConfigValueFactory.fromAnyRef(branch))
          .withValue("parentbranch", ConfigValueFactory.fromAnyRef(fromBranch))
          .withValue("status", ConfigValueFactory.fromAnyRef("init"))
          .withValue("lastversion", ConfigValueFactory.fromAnyRef("1"))
        branchConfig.addContent(branchCfgContent.root().render(ConfigRenderOptions.concise()))
        import spark.implicits._
        val resultSeq = Seq(BranchInitResult(branch, model, branchPath, "Initialization is succcesfull"))
        resultSeq.toDF
      } else {
        sendError("Dir for version 1 not created")
      }
    } else {
      sendError("Dir for branch not created")
    }
  }
}

case class BranchInitResult(name: String, model: String, path: String, workMessage: String)
