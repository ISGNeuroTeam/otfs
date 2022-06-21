package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import java.io.File
import scala.collection.JavaConverters.asJavaIterableConverter

class FSPut(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  private val mode = getKeyword("mode") match {
    case Some("append") => SaveMode.Append
    case Some("overwrite") => SaveMode.Overwrite
    case Some("ignore") => SaveMode.Ignore
    case Some(_) => sendError("Specified save mode is not supported")
    case _ => SaveMode.Overwrite
  }

  private val partitionBy = getKeyword("partitionBy").map(_.split(",").map(_.trim))

  private var branch: String = "main"

  val format = ConfigFactory.parseFile(new File(modelPath + "/format.conf")).getString("format")

  private def castNullColsToString(df: DataFrame): DataFrame = {
    val schema = df.schema
    val colsCasted = df.columns.map{
      colName => if (schema(colName).dataType == NullType) col(colName).cast("String") else col(colName)
    }
    df.select(colsCasted: _*)
  }

  private def createDfWriter: DataFrame => DataFrameWriter[Row] =
    df => df.write.format(format).mode(mode).option("header", "true")

  private def isAllColsExists: (Seq[String], DataFrame) => Boolean =
    (partitions, df) => partitions.forall(df.columns.contains)

  private def getMissedCols: (Seq[String], DataFrame) => Seq[String] =
    (partitions, df) => partitions.filterNot(df.columns.contains)

  private def createNewVersionPlace(lastVersion: Int): Boolean = {
    val branchDir = modelPath + "/" + branch + "/"
    val versionText = (lastVersion + 1).toString
    val newVerDir = new File(branchDir + versionText)
    val newVerDirExists = newVerDir.exists()
    if (newVerDirExists) {
      sendError("Structure error: Version " + (lastVersion + 1).toString + " in branch " + branch + " in model " + model + " is exists, but there is no entry in branch conf file.")
    } else {
      if (newVerDir.mkdirs()) {
        log.info("Directory for version " + versionText + " in branch " + branch + " in model " + model + " created.")
        true
      } else {
        sendError("Directory for version " + versionText + " in branch " + branch + " in model " + model + " not created.")
      }
    }
  }

  override def transform(_df: DataFrame): DataFrame = {
    val dfw = (castNullColsToString _ andThen createDfWriter)(_df)
    checkModelExisting
    branch = extractBranchName("branch")
    log.debug("Defined branch: " + branch)
    val branchConfig = new BranchConfig(modelPath, branch)
    val branchPath = "/" + branch
    log.debug("Defined branch path: " + branchPath)
    val lastVersion = branchConfig.getLastVersion().getOrElse("1")
    log.debug("Defined last version: " + lastVersion)
    val branchStatus = branchConfig.getStatus().getOrElse()
    log.debug("Defined branch status: " + branchStatus)
    val isNewVersion = getKeyword("newversion").getOrElse(
      branch match {
        case "main" => if(branchStatus == "init") "false" else {"true"}
        case _ => "false"
      }
    )
    val versionText = isNewVersion match {
      case "true" =>
        if(createNewVersionPlace(lastVersion.toInt))
          (lastVersion.toInt + 1).toString
      case "false" =>
        if (branch == "main" && branchStatus != "init")
          sendError("Writing to main branch always in new version.")
        else
          lastVersion
    }
    val versionPath = "/" + versionText
    val dataPath = modelPath + branchPath + versionPath
    partitionBy match {
      case Some(partitions) if isAllColsExists(partitions, _df) =>
        dfw.partitionBy(partitions: _*).save(dataPath)

      case Some(partitions) =>
        val missedCols = getMissedCols(partitions, _df)
        sendError(s"Missed columns: '${missedCols.mkString(", ")}")

      case _ => dfw.save(dataPath)
    }
    log.info("Data saved in model " + model + ", branch " + branch + ", version " + versionText + ".")
    if (branchStatus == "init") {
      branchConfig.resetConfig("status", "hasData")
      log.debug("Status changed from init to hasData in branch " + branch + ".")
    }
    if (isNewVersion == "true") {
      val newVersion = (lastVersion.toInt + 1).toString
      branchConfig.resetConfig("lastversion", newVersion)
      log.debug("Last version changed from " + lastVersion + " to " + versionText + " in branch " + branch + ".")
      branchConfig.addToListConfig("versions", Array(newVersion).toIterable.asJava)
      log.debug("Version " + versionText + " added to versions list of branch " + branch + ".")
    }
    _df
  }
}