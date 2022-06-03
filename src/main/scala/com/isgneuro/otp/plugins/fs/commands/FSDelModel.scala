package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import scala.reflect.io.Directory

class FSDelModel(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession{
  override def transform(_df: DataFrame): DataFrame = {
    val modelPath = getmodelPath
    val modelExists = new File(modelPath).exists
    if(!modelExists)
      sendError("Model " + model + " doesn't exists.")
    val modelFile = new File(modelPath)
    if (modelFile.exists()) {
      if (modelFile.isDirectory) {
        val modelDirectory = new Directory(modelFile)
        val modelDirDeleted = modelDirectory.deleteRecursively
        if (modelDirDeleted) {
          import spark.implicits._
          val resultSeq = Seq(ModelDelResult(model, "Deleting is successful"))
          resultSeq.toDF
        } else {
          sendError("Directory for model " + model + " not deleted")
        }
      } else {
        sendError("Error: path to model " + model + "isn't directory")
      }
    } else {
      sendError("Directory of model " + model + " isn't exists")
    }
  }
}

case class ModelDelResult(name: String, workMessage: String)
