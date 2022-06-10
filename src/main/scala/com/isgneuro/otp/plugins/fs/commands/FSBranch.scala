package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.{BranchConfig, ModelConfig}
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import scala.collection.JavaConverters.asJavaIterableConverter

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
      log.info("Directory for branch " + branch + " in model " +  model + " created.")
      val version1Path = branchPath + "/1"
      val version1Dir = new File(version1Path)
      val version1CreateSucc = version1Dir.mkdirs()
      if (version1CreateSucc){
        log.info("Directory for version 1 in branch " + branch + " in model " +  model + " created.")
        val branchConfig = new BranchConfig(modelPath, branch)
        log.info("Config files for branch " + branch + " in model " +  model + " created.")
        branchConfig.createConfig("name", branch)
        log.debug("Writing name config")
        branchConfig.createConfig("parentbranch", fromBranch)
        log.debug("Writing parentbranch config")
        branchConfig.createConfig("status", "init")
        log.debug("Writing status config")
        branchConfig.createConfig("lastversion", "1")
        log.debug("Writing lastversion config")
        val versions: java.lang.Iterable[String] = Array("1").toIterable.asJava
        branchConfig.createListConfig("versions", versions)
        log.debug("Writing versions config")
        val branchArray = Array(branch).toIterable.asJava
        val parentBranchConfig = new BranchConfig(modelPath, fromBranch)
        parentBranchConfig.createOrAddToList("childbranches", branchArray)
        log.info("Config files of parent branch " + fromBranch + " added by information about child branch " + branch)
        val modelConfig = new ModelConfig(modelPath)
        modelConfig.addToListConfig("branches", branchArray)
        log.info("Config files of model " + model + " added by information about new branch " + branch)
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
