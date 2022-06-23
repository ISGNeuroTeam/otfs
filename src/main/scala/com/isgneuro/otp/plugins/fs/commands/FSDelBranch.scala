package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.{BranchConfig, ModelConfig}
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import java.io.File
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Directory

class FSDelBranch(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession {

  private val modelConfig = new ModelConfig(modelPath)

  override def transform(_df: DataFrame): DataFrame = {
    //Defining model and branch
    checkModelExisting
    val branch = extractBranchName("branch")
    if (branch == "main") {
      sendError("Main branch has structure value for model and delete only when model delete fully.")
    }
    val branchPath = modelPath + "/" + branch
    val branchDirFile = new File(branchPath)
    if (branchDirFile.exists()) {
      if (branchDirFile.isDirectory) {
        val delResults = new ArrayBuffer[BranchDelResult]
        log.info("Deleting work for branch " + branch + " started.")
        //Child branches defining
        val branchConfig = new BranchConfig(modelPath, branch)
        val childBranches = branchConfig.getChildBranches.getOrElse(Array[String]())
        // Child branches deleting
        if (childBranches.nonEmpty) {
          val childDelPreResults = for {
            br <- childBranches
          } yield deleteChildBranch(br)
          //Child deleting results collecting
          val childDelResults = new ArrayBuffer[BranchDelResult]()
          for {r <- childDelPreResults} {
            childDelResults ++= r
          }
          if (childDelResults.map(_.workMessage).contains("Deleting is failed"))
            sendError("Any child branches in branch " + branch + " not deleted")
          delResults ++= childDelResults
        }
        //Parent branch of deleting branch defining
        val parentBranch = branchConfig.getParentBranch.getOrElse("")
        val parentBranchConfig = new BranchConfig(modelPath, parentBranch)
        //Deleting branch directory defining
        val branchDirectory = new Directory(branchDirFile)
        //Deleting of branch
        val branchDirDeleted = branchDirectory.deleteRecursively()
        if (branchDirDeleted) {
          log.debug("Structure of branch " + branch + " deleted")
          //Deleting of entry about deleted branch from parent branch config
          val delBranchNameArray = Array(branch).toIterable.asJava
          parentBranchConfig.removeFromListConfig("childbranches", delBranchNameArray)
          log.debug("Entry about branch " + branch + " deleted from parent branch " + parentBranch + " config.")
          //Deleting of entries about all deleted branches from model config
          modelConfig.removeFromListConfig("branches", delBranchNameArray)
          //Result info table creating
          import spark.implicits._
          delResults += BranchDelResult(branch, model, "Deleting is successful")
          log.debug("Added entry about " + branch + " to result.")
          log.info("Branch " + branch + " deleted with all child branches, all entries, including branch and it's dependencies, in config files removed.")
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

  /**
   * Deleted child branch of any branch with all child branches on all levels
   * @param branch branch name
   * @return info results - all deleted branches
   */
  private def deleteChildBranch(branch: String): Array[BranchDelResult] = {
    val result = new ArrayBuffer[BranchDelResult]
    val brDirFile = new File(modelPath + "/" + branch)
    val branchConfig = new BranchConfig(modelPath, branch)
    //Child branches defining and delete childs for all of them recursively with results collecting
    val childBranches = branchConfig.getChildBranches.getOrElse(Array[String]())
    if (childBranches.nonEmpty) {
      val childsResult = for {cb <- childBranches} yield deleteChildBranch(cb)
      result ++= childsResult.flatMap(a => a.map(r => r))
    }
    //Deleted branch, passed in parameter
    val brDirDeleted = deleteDirectory(brDirFile)
    if (brDirDeleted){
      log.debug("Structure of child branch " + branch + " deleted")
      val delBranchNameArray = Array(branch).toIterable.asJava
      modelConfig.removeFromListConfig("branches", delBranchNameArray)
      log.debug("Entry about branch " + branch + " deleted from model config.")
    }
    val workMessage = if (brDirDeleted) {
      "Deleting is successful"
    } else {
      "Deleting is failed"
    }
    //Result forming
    result += BranchDelResult(branch, model, workMessage)
    log.debug("Added entry about " + branch + " to result.")
    result.toArray
  }

  private def deleteDirectory(branchDirFile: File): Boolean = {
    val branchDirectory = new Directory(branchDirFile)
    branchDirectory.deleteRecursively()
  }

}

case class BranchDelResult(name: String, model: String, workMessage: String)
