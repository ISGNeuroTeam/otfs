package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import com.isgneuro.otp.plugins.fs.internals.Storage
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import java.io.File

class FSPut(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  private val mode = getKeyword("mode") match {
    case Some("append") => SaveMode.Append
    case Some("overwrite") => SaveMode.Overwrite
    case Some("ignore") => SaveMode.Ignore
    case Some(_) => sendError("Specified save mode is not supported")
    case _ => SaveMode.Overwrite
  }

  private val partitionBy = getKeyword("partitionBy").map(_.split(",").map(_.trim))

  private val modelPath = getmodelPath

  private var branch: String = "main"

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
    val newVerDir = new File(branchDir + (lastVersion + 1).toString)
    val newVerDirExists = newVerDir.exists()
    if (newVerDirExists) {
      sendError("Structure error: Version " + (lastVersion + 1).toString + " in branch " + branch + " in model " + model + " is exists, but there is no entry in branch conf file")
    } else {
      newVerDir.mkdirs()
    }
  }

  override def transform(_df: DataFrame): DataFrame = {
    val dfw = (castNullColsToString _ andThen createDfWriter)(_df)
    val branchText = getKeyword("branch").getOrElse("main")
    if (branchText != "main") {
      val branchExists = new File(modelPath + "/" + branchText).exists()
      branch = branchExists match {
        case true => branchText
        case false => sendError("Branch " + branchText + " doesn't exists in model " + model + ". Use command fsbranch for new branch creation")
      }
    }
    val branchConfig = new BranchConfig
    val branchPath = "/" + branch
    val lastVersion = branchConfig.getLatestVersion(modelPath, branch)
    val branchStatus = branchConfig.getStatus(modelPath, branch)
    val isNewVersion = getKeyword("newVersion").getOrElse(
      branch match {
        case "main" => if(branchStatus == "init") "false" else {"true"}
        case _ => "false"
      }
    )
    val versionPath = "/" + (isNewVersion match {
      case "true" =>
        if(createNewVersionPlace(lastVersion.toInt))
          (lastVersion.toInt + 1).toString
      case "false" =>
        if (branch == "main" && branchStatus != "init")
          sendError("Writing to main branch always in new version.")
        else
          lastVersion
    })
    val dataPath = modelPath + branchPath + versionPath
    partitionBy match {
      case Some(partitions) if isAllColsExists(partitions, _df) =>
        dfw.partitionBy(partitions: _*).save(dataPath)

      case Some(partitions) =>
        val missedCols = getMissedCols(partitions, _df)
        sendError(s"Missed columns: '${missedCols.mkString(", ")}")

      case _ => dfw.save(dataPath)
    }
    if (branchStatus == "init") {

    }
    _df
  }
}