/*package com.isgneuro.otp.plugins.fs.commands

import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class _5FSMergeTest extends CommandTest{
  override val dataset: String = """[
                                   |{"a":"1","b":"2"},
                                   |{"a":"10","b":"20"}
                                   |]""".stripMargin

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

  test("Merge") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val outBranchPath = modelPath + "/branch1"
      val inBranchPath = modelPath + "/branch2"
      if (!new File(outBranchPath).exists() || !new File(inBranchPath).exists()) {
        log.error("As   minimum one of branches in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel outbranch=branch1 inbranch=branch2""")
        val commandWriteFile = new FSMerge(simpleQuery, utils)
        execute(commandWriteFile)

        val actual = spark.read.format("parquet").load(inBranchPath + "/2").toJSON.collect().mkString("[\n",",\n","\n]")
        val actualDf = jsonToDf(actual)
        val mergedDf = jsonToDf(appended).union(jsonToDf(dataset))
        assert(actualDf.except(mergedDf).count() == 0)
      }
    }
  }

  test("Merge with outbranchversion defining") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val outBranchPath = modelPath + "/main"
      val inBranchPath = modelPath + "/branch2"
      if (!new File(outBranchPath).exists() || !new File(inBranchPath).exists()) {
        log.error("As minimum one of branches in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel outbranch=main inbranch=branch2 outbranchversion=2""")
        val commandWriteFile = new FSMerge(simpleQuery, utils)
        execute(commandWriteFile)

        val actual = spark.read.format("parquet").load(inBranchPath + "/2").toJSON.collect().mkString("[\n",",\n","\n]")
        val actualDf = jsonToDf(actual)
        val mergedDf = jsonToDf(appended).union(jsonToDf(dataset)).union(jsonToDf(dataset))
        assert(actualDf.except(mergedDf).count() == 0)
      }
    }
  }

  test("Merge with inbranchversion defining") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val outBranchPath = modelPath + "/branch1"
      val inBranchPath = modelPath + "/branch2"
      if (!new File(outBranchPath).exists() || !new File(inBranchPath).exists()) {
        log.error("As minimum one of branches in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel outbranch=branch1 inbranch=branch2 inbranchversion=1""")
        val commandWriteFile = new FSMerge(simpleQuery, utils)
        execute(commandWriteFile)

        val actual = spark.read.format("parquet").load(inBranchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val actualDf = jsonToDf(actual)
        val mergedDf = jsonToDf(dataset).union(jsonToDf(dataset))
        assert(actualDf.except(mergedDf).count() == 0)
      }
    }
  }

  test("Merge with out- and inbranchversion defining") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val outBranchPath = modelPath + "/main"
      val inBranchPath = modelPath + "/branch2"
      if (!new File(outBranchPath).exists() || !new File(inBranchPath).exists()) {
        log.error("As minimum one of branches in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testModel outbranch=main inbranch=branch2 outbranchversion=3 inbranchversion=1""")
        val commandWriteFile = new FSMerge(simpleQuery, utils)
        execute(commandWriteFile)

        val actual = spark.read.format("parquet").load(inBranchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val actualDf = jsonToDf(actual)
        val mergedDf = jsonToDf(dataset).union(jsonToDf(dataset)).union(jsonToDf(dataset))
        assert(actualDf.except(mergedDf).count() == 0)
      }
    }
  }

  test("Merge with not existing version") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val outBranchPath = modelPath + "/branch1"
      val inBranchPath = modelPath + "/branch2"
      if (!new File(outBranchPath).exists() || !new File(inBranchPath).exists()) {
        log.error("As minimum one of branches in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testModel outbranch=branch1 inbranch=branch2 inbranchversion=35""")
        val thrown = intercept[Exception] {
          val commandWriteFile = new FSMerge(simpleQuery, utils)
          execute(commandWriteFile)
        }
        assert(thrown.getMessage.contains("Version 35 doesn't exists in branch branch2."))
      }
    }
  }
}*/
