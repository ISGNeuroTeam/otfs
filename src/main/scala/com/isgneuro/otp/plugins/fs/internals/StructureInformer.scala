package com.isgneuro.otp.plugins.fs.internals

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.model.Branch
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.BasicFileAttributes

class StructureInformer(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) with OTLSparkSession{

  protected var showVersionsList = "false"

  protected var showDataExistsInfo = "false"

  protected var showCreationDate = "false"

  protected var showLastUpdateDate = "false"

  protected var showLastVersionNum = "false"

  protected var hasChildBranches = "false"

  protected var onlyEmpty = "false"

  protected var onlyNonEmpty = "false"

  protected var onlyWithChildBranches = "false"

  protected var onlyWithoutChildBranches = "false"

  //protected val config = new BranchConfig

  private val allowedLogicParamValues = Array("true", "false")

  protected def getLogicParamValue(name: String): String = {
    val result = getKeyword(name).getOrElse("false")
    if (!allowedLogicParamValues.contains(result))
      sendError("Not allowed value for parameter " + name)
    result
  }

  protected def is(logicText: String): Boolean = {
    logicText match {
      case "true" => true
      case _ => false
    }
  }

  protected def getIsChildBranchesExistsText(modelPath: String, branch: String): String = {
    if (childBranchesExistsIn(modelPath, branch)) "y" else "n"
  }

  protected def childBranchesExistsIn(modelPath: String, branch: String): Boolean = {
    val branchConfig = new BranchConfig(modelPath, branch)
    branchConfig.getChildBranches().getOrElse(Array[String]()).length > 0
  }

  protected def getIsDataContainsText(modelPath: String, branch: String): String = {
    if (dataContainsIn(new File(modelPath + "/" + branch))) "y" else "n"
  }

  protected def dataContainsIn(dir: File): Boolean = {
    if (dir.isDirectory) {
      dir.listFiles.filter(_.isDirectory).forall(_.listFiles.length > 0)
    } else {
      sendError("Checking file on directory properties.")
    }
  }

  protected def getBranchDirs(branchDirCandids: Array[File]): Array[File] = {
    if (onlyEmpty == "true") {
      branchDirCandids.filter(dir => dir.listFiles.count(_.isDirectory) == 0 || dir.listFiles.filter(_.isDirectory).forall(_.listFiles.length == 0))
    } else if (onlyNonEmpty == "true") {
      branchDirCandids.filter(dataContainsIn)
    } else if (onlyWithChildBranches == "true") {
      branchDirCandids.filter(dir => childBranchesExistsIn(modelPath, dir.getName))
    } else if (onlyWithoutChildBranches == "true") {
      branchDirCandids.filter(dir => !childBranchesExistsIn(modelPath, dir.getName))
    } else
      branchDirCandids
  }

  protected def createBranchesDataframe(branchNames: Array[String]): DataFrame = {
    val branches = for {
      br <- branchNames
      config = new BranchConfig(modelPath, br)
      parent = config.getParentBranch().getOrElse("")
    } yield Branch(br, parent)
    import spark.implicits._
    branches.toSeq.toDF()
  }

  protected def completeBranchesDataframe(branchesDf: DataFrame): DataFrame = {
    var branches = branchesDf
    if (is(showVersionsList)) {
      val versionsListUdf = udf((name: String) => {new BranchConfig(modelPath, name).getVersionsList().getOrElse(Array[String]()).mkString(",")})
      branches = branches.withColumn("versions", versionsListUdf(col("name")))
    }
    if (is(showLastVersionNum)) {
      val lastVersionUdf = udf((lvName: String) => {new BranchConfig(modelPath, lvName).getLastVersion().getOrElse("")})
      branches = branches.withColumn("lastVersion", lastVersionUdf(col("name")))
    }
    if (is(showDataExistsInfo)) {
      val isDataExistsUdf = udf((deName: String) => {getIsDataContainsText(modelPath, deName)})
      branches = branches.withColumn("isDataExists", isDataExistsUdf(col("name")))
    }
    if (is(showCreationDate)) {
      val creationDateUdf = udf((cdName: String) => {Files.readAttributes(Paths.get(modelPath + "/" + cdName), classOf[BasicFileAttributes]).creationTime().toString})
      branches = branches.withColumn("creationDate", creationDateUdf(col("name")))
    }
    if (is(showLastUpdateDate)) {
      val lastUpdateDateUdf = udf((udName: String) => {Files.getLastModifiedTime(Paths.get(modelPath + "/" + udName)).toString})
      branches = branches.withColumn("lastUpdateDate", lastUpdateDateUdf(col("name")))
    }
    if (is(hasChildBranches)) {
      val hasChildBranchesUdf = udf((hcName: String) => {getIsChildBranchesExistsText(modelPath, hcName)})
      branches = branches.withColumn("hasChildBranches", hasChildBranchesUdf(col("name")))
    }
    branches
  }
}
