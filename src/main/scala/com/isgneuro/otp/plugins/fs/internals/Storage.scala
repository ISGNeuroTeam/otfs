package com.isgneuro.otp.plugins.fs.internals

import java.io.File

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

class Storage(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {

  val format: String = getKeyword("format").getOrElse("parquet")
  val path: String = getKeyword("path") match {
    case Some(p) => p.replace("../","")
    case None => sendError("No path specified")
  }
  val fs: String = pluginConfig.getString("storage.fs")
  val basePath: String = pluginConfig.getString("storage.path")
  val absolutePath: String = fs + new File(basePath,path).getAbsolutePath
  log.info(s"Absolute path: $absolutePath. Format: $format")

  val requiredKeywords: Set[String] = Set("path")
  val optionalKeywords: Set[String] = Set("format")

  override def transform(_df: DataFrame): DataFrame = _df
}


