package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.internals.Storage
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

import scala.util.Try

class FSGet(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  private val isInferSchema: String = getKeyword("inferSchema").getOrElse("true")

  private val delimiter: Option[String] = getKeyword("sep").map(
    value => value.stripPrefix("\"").stripSuffix("\"")
  )

  private def appendInferSchema: DataFrameReader => DataFrameReader = dfr =>
    if (format == "csv") dfr.option("inferSchema", isInferSchema) else dfr

  private def appendDelimiter: DataFrameReader => DataFrameReader = dfr => delimiter match {
    case Some(sep) => dfr.option("delimiter", sep)
    case None => dfr
  }

  override def transform(_df: DataFrame): DataFrame = {
    val sparkSession = Try(_df.sparkSession)
      .getOrElse(SparkSession.builder().getOrCreate())
    val commonReader: DataFrameReader = sparkSession.read
      .format(format)
      .option("header", "true")

    val dfReader = appendInferSchema andThen appendDelimiter
    dfReader(commonReader).load(absolutePath)
  }
}