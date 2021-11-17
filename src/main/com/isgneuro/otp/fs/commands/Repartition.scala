package com.isgneuro.otp.fs.commands

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.util.{Failure, Success, Try}

/** OTL command. It changes number of partition in dataframe.
 * @param sq [[SimpleQuery]] query object.
 * @author Sergey Ermilov (sermilov@ot.ru)
 */
class Repartition(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {
  val requiredKeywords: Set[String] = Set("num")

  def transform(_df: DataFrame): DataFrame =  {
    val numPartitions = Try(getKeyword("num").get) match {
      case Success(x) => x
      case Failure(_)=> sendError("The value of parameter 'num' should be specified")

    }
    val num = Try(numPartitions.toInt) match {
      case Success(x) => x
      case Failure(_) => sendError("The value of parameter 'num' should be of int type")
    }
    _df.repartition(num)
  }
}
