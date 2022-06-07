package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.StructureInformer
import com.isgneuro.otp.spark.OTLSparkSession
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.sdk.core.SimpleQuery
import org.apache.spark.sql.functions._
import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths}

class FSGetBranches(sq: SimpleQuery, utils: PluginUtils) extends StructureInformer(sq, utils) with OTLSparkSession {

  override def transform(_df: DataFrame): DataFrame = {
    val showDataExistsInfo = getLogicParamValue("showdataexistsinfo")
    val showCreationDate = getLogicParamValue("showcreationdate")
    val showLastUpdateDate = getLogicParamValue("showlastupdatedate")
    val showLastVersionNum = getLogicParamValue("showlastversionnum")
    val hasChildBranches = getLogicParamValue("haschildbranches")
    val onlyEmpty = getLogicParamValue("onlyempty")
    val onlyNonEmpty = getLogicParamValue("onlynonempty")
    if (onlyEmpty == "true" && onlyEmpty == onlyNonEmpty) {
      sendError("Error: equal values in opposite parameters onlyempty and onlynonempty. Define various values in these params or define only one param.")
    }
    val onlyWithChildBranches = getLogicParamValue("onlywithchildbranches")
    val onlyWithoutChildBranches = getLogicParamValue("onlywithoutchildbranches")
    if (onlyWithChildBranches == "true" && onlyWithChildBranches == onlyWithoutChildBranches) {
      sendError("Error: equal values in opposite parameters onlyWithChildBranches and onlyWithoutChildBranches. Define various values in these params or define only one param.")
    }
    val showVersionsList = getLogicParamValue("showversionslist")
    val modelPath = getmodelPath
    val modelExists = new File(modelPath).exists
    if(!modelExists)
      sendError("Model " + model + " doesn't exists.")
    val modelDirectory = new File(modelPath)
    var branchDirs: Array[File] = modelDirectory.listFiles.filter(_.isDirectory)
    if (onlyEmpty == "true") {
      branchDirs = branchDirs.filter(dir => dir.listFiles.count(_.isDirectory) == 0 || dir.listFiles.filter(_.isDirectory).forall(_.listFiles.length == 0))
    }
    if (onlyNonEmpty == "true") {
      branchDirs = branchDirs.filter(dataContainsIn)
    }
    if (onlyWithChildBranches == "true") {
      branchDirs = branchDirs.filter(dir => childBranchesExistsIn(modelPath, dir.getName))
    }
    if (onlyWithoutChildBranches == "true") {
      branchDirs = branchDirs.filter(dir => !childBranchesExistsIn(modelPath, dir.getName))
    }
    val branchNames: Array[String] = branchDirs.map(_.getName)
    val config = new BranchConfig
    val branches = for {
      br <- branchNames
      parent = config.getParentBranch(modelPath, br).getOrElse("")
    } yield Branch(br, parent)
    import spark.implicits._
    var branchesDf = branches.toSeq.toDF()
    if (is(showVersionsList)) {
      val versionsListUdf = udf((name: String) => {config.getVersionsList(modelPath, name).getOrElse(Array[String]()).mkString(",")})
      branchesDf = branchesDf.withColumn("versions", versionsListUdf(col("name")))
    }
    if (is(showLastVersionNum)) {
      val lastVersionUdf = udf((lvName: String) => {config.getLastVersion(modelPath, lvName).getOrElse("")})
      branchesDf = branchesDf.withColumn("lastVersion", lastVersionUdf(col("name")))
    }
    if (is(showDataExistsInfo)) {
      val isDataExistsUdf = udf((deName: String) => {getIsDataContainsText(modelPath, deName)})
      branchesDf = branchesDf.withColumn("isDataExists", isDataExistsUdf(col("name")))
    }
    if (is(showCreationDate)) {
      val creationDateUdf = udf((cdName: String) => {Files.readAttributes(Paths.get(modelPath + "/" + cdName), classOf[BasicFileAttributes]).creationTime().toString})
      branchesDf = branchesDf.withColumn("creationDate", creationDateUdf(col("name")))
    }
    if (is(showLastUpdateDate)) {
      val lastUpdateDateUdf = udf((udName: String) => {Files.getLastModifiedTime(Paths.get(modelPath + "/" + udName)).toString})
      branchesDf = branchesDf.withColumn("lastUpdateDate", lastUpdateDateUdf(col("name")))
    }
    if (is(hasChildBranches)) {
      val hasChildBranchesUdf = udf((hcName: String) => {getIsChildBranchesExistsText(modelPath, hcName)})
      branchesDf = branchesDf.withColumn("hasChildBranches", hasChildBranchesUdf(col("name")))
    }
    branchesDf
  }

}

case class Branch(name: String, parent: String)
