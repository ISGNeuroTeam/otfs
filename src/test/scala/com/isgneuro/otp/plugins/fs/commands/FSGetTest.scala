package com.isgneuro.otp.plugins.fs.commands

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.test.CommandTest
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSGetTest extends CommandTest {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  override val dataset: String = """[
                                   |{"a":"1","b":"2"},
                                   |{"a":"10","b":"20"}
                                   |]""".stripMargin

  val initialDf: DataFrame = jsonToDf(dataset)

  val appended: String = """[
                           | {"a":"1","b":"2"},
                           |{"a":"10","b":"20"},
                           |{"a":"100","b":"200"}
                           |]""".stripMargin

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  private val df: DataFrame = Seq(
    (1, "qwe"),
    (10, "rty")
  ).toDF("a", "b")

  test ("Read files by default") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      val expectedCols = Array("a", "b")
      assert(actual.columns.sameElements(expectedCols))
      assert(actual.rdd.getNumPartitions == 3)
      assert(actual.except(initialDf).count() == 0)
    }
  }

  test("Read files with defined branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel branch=branch2""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      assert(actual.except(jsonToDf(appended)).count() == 0)
    }
  }

  test("Read files with defined version") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel version=1""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      assert(actual.except(initialDf).count() == 0)
    }
  }

  test("Read files with defined branch and version") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel branch=branch2 version=1""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      assert(actual.except(initialDf).count() == 0)
    }
  }

  test("Infer schema if 'csv' format specified ") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testcsvmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testcsvmodel inferSchema=true""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      val expectedSchema = StructType(
        List(
          StructField("a", IntegerType),
          StructField("b", StringType)
        )
      )

      assert(actual.columns.sameElements(expectedSchema.map(_.name)))
      assert(actual.schema.equals(expectedSchema))
      assert(actual.except(initialDf).count() == 0)
    }
  }

  test("Do not infer schema if 'csv' format specified and inferSchema=false") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testcsvmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testcsvmodel inferSchema=false""")
      val commandReadFile = new FSGet(simpleQuery, utils)
      val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
      val expectedSchema = StructType(
        List(
          StructField("a", StringType),
          StructField("b", StringType)
        )
      )

      assert(actual.columns.sameElements(expectedSchema.map(_.name)))
      assert(actual.schema.equals(expectedSchema))
      assert(actual.except(initialDf).count() == 0)
    }
  }

  test("Read from not existing branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel branch=branch50 version=1""")
      val thrown = intercept[Exception] {
        val commandReadFile = new FSGet(simpleQuery, utils)
        commandReadFile.transform(sparkSession.emptyDataFrame)
      }
      assert(thrown.getMessage.contains("Branch branch50, version 1 doesn't exists or doesn't contains data"))
    }
  }

  test("Read from not existing version") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("""model=testModel branch=branch1 version=10""")
      val thrown = intercept[Exception] {
        val commandReadFile = new FSGet(simpleQuery, utils)
        commandReadFile.transform(sparkSession.emptyDataFrame)
      }
      assert(thrown.getMessage.contains("Branch branch1, version 10 doesn't exists or doesn't contains data"))
    }
  }
}