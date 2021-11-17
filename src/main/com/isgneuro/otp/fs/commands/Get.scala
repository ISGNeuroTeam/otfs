package com.isgneuro.otp.fs.commands

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.isgneuro.otp.fs.internals.Storage
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import scala.util.Try

class Get(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  override def transform(_df: DataFrame): DataFrame = {
    val sparkSession = Try(_df.sparkSession)
      .getOrElse(SparkSession.builder().getOrCreate())
    val commonReader = sparkSession.read
      .format(format)
      .option("header", "true")

    val dfReader = if (format == "parquet") commonReader else commonReader.option("inferSchema", "true")
    dfReader.load(absolutePath)
  }
}