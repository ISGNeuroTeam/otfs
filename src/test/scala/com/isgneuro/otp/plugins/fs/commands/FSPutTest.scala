package com.isgneuro.otp.plugins.fs.commands

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FSPutTest extends CommandTest {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  override val dataset: String = """[
                                   |{"a":"1","b":"2"},
                                   |{"a":"10","b":"20"}
                                   |]""".stripMargin

  val datasetToAppend: String = """[
                                  |{"a":"100","b":"200"}
                                  |]""".stripMargin

  val dataset3cols: String = """[
                               |{"a":"1","b":"2","c":"3"},
                               |{"a":"10","b":"2","c":"30"},
                               |{"a":"10","b":"20","c":"300"}
                               |]""".stripMargin

  val appended: String = """[
                           | {"a":"1","b":"2"},
                           |{"a":"10","b":"20"},
                           |{"a":"100","b":"200"}
                           |]""".stripMargin

  val datasetNulls: String = """[
                               | {"a":"1","b":"2","c":null},
                               |{"a":"10","b":"20","c":null},
                               |{"a":"100","b":"200","c":null}
                               |]""".stripMargin

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  test("Write to main by default") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "hasData")

        val actual = spark.read.format("parquet").load(branchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write to main") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=main""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "2")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))
        assert(branchVersionsConfig.getStringList("versions").contains("2"))

        val actual = spark.read.format("parquet").load(branchPath + "/2").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  ignore("Write to main newVersion=true"){
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=main newversion=true""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "3")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))
        assert(branchVersionsConfig.getStringList("versions").contains("2"))
        assert(branchVersionsConfig.getStringList("versions").contains("3"))
        val actual = spark.read.format("parquet").load(branchPath + "/3").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  ignore("Write to main newVersion=false"){
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testModel branch=main newversion=false""")
        val thrown = intercept[Exception] {
          val commandWriteFile = new FSPut(simpleQuery, utils)
          execute(commandWriteFile)
        }
        assert(thrown.getMessage.contains("Writing to main branch always in new version."))
      }
    }
  }

  test("Write to some branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch1"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=branch1""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "hasData")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val actual = spark.read.format("parquet").load(branchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write to some branch (other, init)") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch2"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=branch2""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "hasData")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val actual = spark.read.format("parquet").load(branchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write to some branch (other, continue)") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch2"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=branch2""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        commandWriteFile.transform(jsonToDf(appended))

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "2")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))
        assert(branchVersionsConfig.getStringList("versions").contains("2"))

        val actual = spark.read.format("parquet").load(branchPath + "/2").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = appended
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write json") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testjsonmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testjsonmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testjsonmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testjsonmodel""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "hasData")

        val actual = spark.read.format("json").load(branchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write csv") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testcsvmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testcsvmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testcsvmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testcsvmodel""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "hasData")

        val actual = spark.read.format("csv").option("header", "true").load(branchPath + "/1").toJSON.collect().mkString("[\n",",\n","\n]")
        val expected = dataset
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Write parquet + partitionBy") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel partitionBy=a""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        execute(commandWriteFile)

        val expected = jsonToDf(dataset)
        val actualDF = spark.read.format("parquet").load(branchPath + "/3").select("a", "b").sort("a")
        assert(actualDF.rdd.getNumPartitions == 2)
        assert(actualDF.except(expected).count() == 0)
      }
    }
  }

  test("Write parquet + partitionBy on multiple columns") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/main"
      if (!new File(branchPath).exists()) {
        log.error("Branch main in model testmodel doesn't exists")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel partitionBy=a,b""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        commandWriteFile.transform(jsonToDf(dataset3cols))

        val expected = jsonToDf(dataset3cols)
        val actualDF = spark.read.format("parquet").load(branchPath + "/4").select("a", "b", "c").sort("a")
        assert(actualDF.rdd.getNumPartitions == 3)
        assert(actualDF.except(expected).count() == 0)
      }
    }
  }

  ignore("Write with different modes") { // TODO Make one test per mode
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch3"
      if (!new File(branchPath).exists()) {
        log.error("Branch branch3 in model testmodel doesn't exists")
      } else {
        execute(jsonToDf(datasetToAppend), new FSPut(SimpleQuery(""" model=testmodel branch=branch3 """), utils))
        execute(jsonToDf(dataset), new FSPut(SimpleQuery(""" model=testmodel branch=branch3 """), utils))
        execute(jsonToDf(datasetToAppend), new FSPut(SimpleQuery(""" model=testmodel branch=branch3 mode=append """), utils))

        val expected = jsonToDf(appended)
        val actualDF = spark.read.format("parquet").load(branchPath + "/1").select("a", "b").sort("a")
        assert(actualDF.except(expected).count() == 0)

        execute(jsonToDf(dataset), new FSPut(SimpleQuery(""" model=testmodel branch=branch3 mode=ignore """), utils))
        val actualDFignored = spark.read.format("parquet").load(branchPath + "/1").select("a", "b").sort("a")
        assert(actualDFignored.except(expected).count() == 0)
      }
    }
  }

  test("Write null column") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch4"
      if (!new File(branchPath).exists()) {
        log.error("Branch branch4 in model testmodel doesn't exists")
      } else {
        val df = Seq(
          (1, "qwe"),
          (2, "rty")
        ).toDF("a", "b").withColumn("c", lit(null))

        val simpleQuery = SimpleQuery("""model=testmodel branch=branch4""")
        val commandWriteFile = new FSPut(simpleQuery, utils)
        commandWriteFile.transform(df)

        val actualReaded = spark.read.format("parquet").load(branchPath + "/1")
        assert(actualReaded.columns.sameElements(Array("a", "b", "c")))
        assert(actualReaded.except(df).count() == 0)
      }
    }
  }

  test("Write to unknown branch") {
    val simpleQuery = SimpleQuery("""model=testmodel branch=superbranch""")
    val thrown = intercept[Exception] {
      val commandWriteFile = new FSPut(simpleQuery, utils)
      execute(commandWriteFile)
    }
    assert(thrown.getMessage.contains("Branch superbranch doesn't exists in model testmodel. Use command fsbranch for new branch creation"))
  }

}