package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.internals.Storage
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import scala.util.Try

class FSGet(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  override def transform(_df: DataFrame): DataFrame = {
    val sparkSession = Try(_df.sparkSession)
      .getOrElse(SparkSession.builder().getOrCreate())
    val commonReader = sparkSession.read
      .format(format)
      .option("header", "true")

    val dfReader = if (format == "csv") commonReader.option("inferSchema", isInferSchema) else commonReader
    dfReader.load(absolutePath)
  }
}