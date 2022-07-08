package com.isgneuro.otp.plugins.fs.commands

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class RepartitionTest extends CommandTest {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  override val dataset: String = """_"""

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  private val df: DataFrame = Seq(
    (1, "qwe"),
    (2, "rty"),
    (3, "uio"),
    (4, "asd"),
    (5, "fgh"),
    (6, "jkl"),
    (7, "zxc"),
    (8, "vbn")
  ).toDF("a", "b")

  test("Change number of partitions") {
    val query = SimpleQuery("num=4")
    val command = new Repartition(query, utils)
    val actual = command.transform(df)
    assert(actual.rdd.getNumPartitions == 4)
  }

  test("Throw an error if no partition number specified") {
    val query = SimpleQuery("")
    val thrown = intercept[Exception] {
      val command = new Repartition(query, utils)
      command.transform(df)
    }
    assert(thrown.getMessage.contains("You should specify number of partitions in 'num' parameter"))
  }

  test("Throw an error if partition number is non integer") {
    val query = SimpleQuery("num=aaa")
    val thrown = intercept[Exception] {
      val command = new Repartition(query, utils)
      command.transform(df)
    }
    assert(thrown.getMessage.contains("You should specify the 'num' parameter as positive integer"))
  }
}