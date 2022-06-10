package com.isgneuro.otp.plugins.fs.commands

import scala.collection.JavaConverters._
import com.isgneuro.otp.plugins.fs.config.{BranchConfig, ModelConfig}
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSInit (sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  override def transform(_df: DataFrame): DataFrame = {
    //create new model dir
    val modelDir = new File(modelPath)
    val dirAlreadyExists = modelDir.exists()
    val dirCreateSucc: Boolean = if (!dirAlreadyExists) {
      modelDir.mkdirs()
    } else
      sendError("Such model is already exists")
    if (dirCreateSucc) {
      log.info("Directory for model " + model + " created.")
      //create dir for main branch
      val mainBranchPath = modelPath + "/main"
      val mainBranchDir = new File(mainBranchPath)
      val mainBranchCreateSucc = mainBranchDir.mkdirs()
      if (mainBranchCreateSucc) {
        log.info("Directory for main branch in model " +  model + " created.")
        //create dir for version 1
        val version1Path = mainBranchPath + "/1"
        val version1Dir = new File(version1Path)
        val version1CreateSucc = version1Dir.mkdirs()
        if (version1CreateSucc) {
          log.info("Directory for version 1 in main branch in model " +  model + " created.")
          val format = getKeyword("format").getOrElse("parquet")
          val branches: java.lang.Iterable[String] = Array("main").toIterable.asJava
          //create model config
          val modelConfig = new ModelConfig(modelPath)
          log.info("Config files for model " +  model + " created.")
          modelConfig.createConfig("model", model)
          log.debug("Writing model config")
          modelConfig.createConfig("format", format)
          log.debug("Writing format config")
          modelConfig.createListConfig("branches", branches)
          log.debug("Writing branches config")
          //create main config
          val branchConfig = new BranchConfig(modelPath, "main")
          log.info("Config files for main branch in model " +  model + " created.")
          branchConfig.createConfig("name", "main")
          log.debug("Writing name config")
          branchConfig.createConfig("mode", "onewrite")
          log.debug("Writing mode config")
          branchConfig.createConfig("status", "init")
          log.debug("Writing status config")
          branchConfig.createConfig("lastversion", "1")
          log.debug("Writing lastversion config")
          val versions: java.lang.Iterable[String] = Array("1").toIterable.asJava
          branchConfig.createListConfig("versions", versions)
          log.debug("Writing versions config")
          //result info table
          import spark.implicits._
          val resultSeq = Seq(InitResult(model, modelPath, "Initialization is succcesfull"))
          resultSeq.toDF
        } else {
          sendError("Dir for version 1 not created")
        }
      } else {
        sendError("Dir for main branch not created")
      }
    } else {
      sendError("Dir for model not created")
    }
  }
}

case class InitResult(name: String, path: String, workMessage: String)
