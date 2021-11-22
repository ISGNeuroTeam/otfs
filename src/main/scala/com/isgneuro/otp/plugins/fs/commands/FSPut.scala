package com.isgneuro.otp.fs.commands

import com.isgneuro.otp.fs.internals.Storage
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.functions.col
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.PluginUtils

class FSPut(sq: SimpleQuery, utils: PluginUtils) extends Storage(sq, utils) {

  private val mode = getKeyword("mode") match {
    case Some("append") => SaveMode.Append
    case Some("overwrite") => SaveMode.Overwrite
    case Some("ignore") => SaveMode.Ignore
    case Some(_) => sendError("Specified save mode is not supported")
    case _ => SaveMode.Overwrite
  }

  private val partitionBy = getKeyword("partitionBy").map(_.split(",").map(_.trim))

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

  override def transform(_df: DataFrame): DataFrame = {
    val dfw = (castNullColsToString andThen createDfWriter)(_df)
    partitionBy match {
      case Some(partitions) if isAllColsExists(partitions, _df) =>
        dfw.partitionBy(partitions: _*).save(absolutePath)

      case Some(partitions) => {
        val missedCols = getMissedCols(partitions, _df)
        sendError(s"Missed columns: '${missedCols.mkString(", ")}")
      }

      case _ => dfw.save(absolutePath)
    }
    _df
  }
}
