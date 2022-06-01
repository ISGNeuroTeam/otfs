package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import scala.reflect.io.Directory

class FSDelBranch(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession{
  private var branch = "main"

  override def transform(_df: DataFrame): DataFrame = {
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
    }
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.isDirectory) {
      val branchDirectory = new Directory(branchDirFile)
      val branchDirDeleted = branchDirectory.deleteRecursively()
      if (branchDirDeleted) {
        import spark.implicits._
        val resultSeq = Seq(BranchDelResult(branch, model, "Deleting is successfull"))
        resultSeq.toDF
      } else {
        sendError("Directory for " + branch + " not deleted.")
      }
    } else {
      sendError("Error: path to branch " + branch + " isn't directory.")
    }
  }
}

case class BranchDelResult (name: String, model: String, workMessage: String)
