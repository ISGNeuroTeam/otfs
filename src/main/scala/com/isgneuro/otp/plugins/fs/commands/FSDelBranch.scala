package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Directory

class FSDelBranch(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession{

  override def transform(_df: DataFrame): DataFrame = {
    checkModelExisting
    val branch = extractBranchName("branch")
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.exists()) {
      if (branchDirFile.isDirectory) {
        val delResults = new ArrayBuffer[BranchDelResult]
        val branchConfig = new BranchConfig
        val childBranches = branchConfig.getChildBranches(modelPath, branch).getOrElse(Array[String]())
        if (childBranches.nonEmpty) {
          val childDelResults = for {
            br <- childBranches
            brDirFile = new File(modelPath + "/" + br)
            brDirDeleted = deleteDirectory(brDirFile)
            workMessage = if (brDirDeleted) {
              "Deleting is successful"
            } else {
              "Deleting is failed"
            }
          } yield BranchDelResult(br, model, workMessage)
          if (childDelResults.map(_.workMessage).contains("Deleting is failed"))
            sendError("Any child branches in branch " + branch + " not deleted")
          delResults ++= childDelResults
        }
        val branchDirectory = new Directory(branchDirFile)
        val branchDirDeleted = branchDirectory.deleteRecursively()
        if (branchDirDeleted) {
          import spark.implicits._
          delResults += BranchDelResult(branch, model, "Deleting is successful")
          delResults.toDF
        } else {
          sendError("Directory for " + branch + " not deleted.")
        }
      } else {
        sendError("Error: path to branch " + branch + " isn't directory.")
      }
    }
    else {
      sendError("Directory of branch " + branch + " isn't exists.")
    }
  }

  private def deleteDirectory(branchDirFile: File): Boolean = {
    val branchDirectory = new Directory(branchDirFile)
    branchDirectory.deleteRecursively()
  }
}

case class BranchDelResult(name: String, model: String, workMessage: String)
