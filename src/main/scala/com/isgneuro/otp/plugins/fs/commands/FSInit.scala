package com.isgneuro.otp.plugins.fs.commands

import scala.collection.JavaConverters._
import com.isgneuro.otp.plugins.fs.config.{MainBranchConfig, ModelConfig}
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}

import java.io.File

class FSInit (sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  override def transform(_df: DataFrame): DataFrame = {
    //create new model dir
    val modelDir = new File(modelPath)
    val dirAlreadyExists = modelDir.exists()
    val dirCreateSucc: Boolean = if (!dirAlreadyExists)
      modelDir.mkdirs()
    else
      sendError("Such model is already exists")
    if (dirCreateSucc) {
      //create dir for main branch
      val mainBranchPath = modelPath + "/main"
      val mainBranchDir = new File(mainBranchPath)
      val mainBranchCreateSucc = mainBranchDir.mkdirs()
      if (mainBranchCreateSucc) {
        //create dir for version 1
        val version1Path = mainBranchPath + "/1"
        val version1Dir = new File(version1Path)
        val version1CreateSucc = version1Dir.mkdirs()
        if (version1CreateSucc) {
          val format = getKeyword("format").getOrElse("parquet")
          //create model config
          val modelConfig = new ModelConfig
          modelConfig.create(modelPath + "/")
          val branches: java.lang.Iterable[String] = Array("main").toIterable.asJava
          val modelCfgContents = Array(ConfigFactory.empty.withValue("model", ConfigValueFactory.fromAnyRef(model))
            .withValue("format", ConfigValueFactory.fromAnyRef(format))
            .withValue("branches", ConfigValueFactory.fromIterable(branches))
          )
          for (content <- modelCfgContents) {
            modelConfig.addContent(content.root().render(ConfigRenderOptions.concise()))
          }
          //create main config
          val mainBranchConfig = new MainBranchConfig
          mainBranchConfig.create(modelPath)
          val mainCfgContent = ConfigFactory.empty.withValue("branchname", ConfigValueFactory.fromAnyRef("main"))
            .withValue("mode", ConfigValueFactory.fromAnyRef("onewrite"))
            .withValue("status", ConfigValueFactory.fromAnyRef("init"))
            .withValue("lastversion", ConfigValueFactory.fromAnyRef("1"))
          mainBranchConfig.addContent(mainCfgContent.root().render(ConfigRenderOptions.concise()))
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
