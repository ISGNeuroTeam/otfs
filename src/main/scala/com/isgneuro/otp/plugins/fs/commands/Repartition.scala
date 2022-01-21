package com.isgneuro.otp.plugins.fs.commands

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

  val numberOfPartitions: String = {
    Try(getKeyword("num").get) match {
      case Success(x) => x
      case Failure(_) => sendError("You should specify number of partitions in 'num' parameter")
    }
  }

  private def castStringToInt: String => Int = (s: String) => {
    Try(s.toInt) match {
      case Success(x) if x > 0 => x
      case _ => sendError("You should specify the 'num' parameter as positive integer")
    }
  }

  def transform(_df: DataFrame): DataFrame =  {
    _df.repartition(
      castStringToInt(numberOfPartitions)
    )
  }
}