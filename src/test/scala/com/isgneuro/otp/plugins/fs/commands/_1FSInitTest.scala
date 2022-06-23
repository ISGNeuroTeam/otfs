package com.isgneuro.otp.plugins.fs.commands

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File
import scala.collection.JavaConverters.asJavaIterableConverter

class _1FSInitTest extends CommandTest {
  val dataset: String = ""

  test("ATest 0. Create new model with default files format") {
    val name = this.getClass.getName
    val simpleName = this.getClass.getSimpleName
    val cannonicalName = this.getClass.getCanonicalName
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (modelDir.exists()) {
      assert(modelDir.exists())
      assert(modelDir.isDirectory)
      log.info("Model testmodel is already exists")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel")
      val initCommand = new FSInit(simpleQuery, utils)
      val actual = execute(initCommand)
      assert(modelDir.exists())
      assert(modelDir.isDirectory)
      val modelConfFile = new File(modelPath + "/model.conf")
      assert(modelConfFile.exists())
      val modelConfig: Config = ConfigFactory.parseFile(modelConfFile)
      assert(modelConfig.getString("model") == "testmodel")

      val formatConfFile = new File(modelPath + "/format.conf")
      assert(formatConfFile.exists())
      val formatConfig: Config = ConfigFactory.parseFile(formatConfFile)
      assert(formatConfig.getString("format") == "parquet")

      val branchesConfFile = new File(modelPath + "/branches.conf")
      assert(branchesConfFile.exists())
      val branchesConfig: Config = ConfigFactory.parseFile(branchesConfFile)
      assert(branchesConfig.getStringList("branches").contains("main"))

      val mainBranchPath = modelPath + "/main"
      val mainBranchDir = new File(mainBranchPath)
      assert(mainBranchDir.exists())
      assert(mainBranchDir.isDirectory)

      val mainBranchNameConfFile = new File(mainBranchPath + "/name.conf")
      assert(mainBranchNameConfFile.exists())
      val mainBranchNameConfig: Config = ConfigFactory.parseFile(mainBranchNameConfFile)
      assert(mainBranchNameConfig.getString("name") == "main")

      val mainBranchModeConfFile = new File(mainBranchPath + "/mode.conf")
      assert(mainBranchModeConfFile.exists())
      val mainBranchModeConfig: Config = ConfigFactory.parseFile(mainBranchModeConfFile)
      assert(mainBranchModeConfig.getString("mode") == "onewrite")

      val mainBranchStatusConfFile = new File(mainBranchPath + "/status.conf")
      assert(mainBranchStatusConfFile.exists())
      val mainBranchStatusConfig: Config = ConfigFactory.parseFile(mainBranchStatusConfFile)
      assert(mainBranchStatusConfig.getString("status") == "init")

      val mainBranchVersionConfFile = new File(mainBranchPath + "/lastversion.conf")
      assert(mainBranchVersionConfFile.exists())
      val mainBranchVersionConfig: Config = ConfigFactory.parseFile(mainBranchVersionConfFile)
      assert(mainBranchVersionConfig.getString("lastversion") == "1")

      val mainBranchVersionsConfFile = new File(mainBranchPath + "/versions.conf")
      assert(mainBranchVersionsConfFile.exists())
      val mainBranchVersionsConfig: Config = ConfigFactory.parseFile(mainBranchVersionsConfFile)
      assert(mainBranchVersionsConfig.getStringList("versions").contains("1"))

      val version1Path = mainBranchPath + "/1"
      val version1Dir = new File(version1Path)
      assert(version1Dir.exists())
      assert(version1Dir.isDirectory)

      val modelBranchesFile = new File(modelPath + "/branches.conf")
      assert(modelBranchesFile.exists())
      val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
      assert(modelBranchesConfig.getStringList("branches").contains("main"))
      val expected =
        """[
          |{"name":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel","workMessage":"Initialization is successful"}
          |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }
  }

  test("ATest 1. Create new model with user defined files format (csv)") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testcsvmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (modelDir.exists()) {
      assert(modelDir.exists())
      assert(modelDir.isDirectory)
    }
    val simpleQuery = SimpleQuery("model=testcsvmodel format=csv")
    val initCommand = new FSInit(simpleQuery, utils)
    val actual = execute(initCommand)
    assert(modelDir.exists())
    assert(modelDir.isDirectory)

    val modelConfFile = new File(modelPath + "/model.conf")
    assert(modelConfFile.exists())
    val modelConfig: Config = ConfigFactory.parseFile(modelConfFile)
    assert(modelConfig.getString("model") == "testcsvmodel")

    val formatConfFile = new File(modelPath + "/format.conf")
    assert(formatConfFile.exists())
    val formatConfig: Config = ConfigFactory.parseFile(formatConfFile)
    assert(formatConfig.getString("format") == "csv")

    val branchesConfFile = new File(modelPath + "/branches.conf")
    assert(branchesConfFile.exists())
    val branchesConfig: Config = ConfigFactory.parseFile(branchesConfFile)
    assert(branchesConfig.getStringList("branches").contains("main"))

    val mainBranchPath = modelPath + "/main"
    val mainBranchDir = new File(mainBranchPath)
    assert(mainBranchDir.exists())
    assert(mainBranchDir.isDirectory)

    val mainBranchNameConfFile = new File(mainBranchPath + "/name.conf")
    assert(mainBranchNameConfFile.exists())
    val mainBranchNameConfig: Config = ConfigFactory.parseFile(mainBranchNameConfFile)
    assert(mainBranchNameConfig.getString("name") == "main")

    val mainBranchModeConfFile = new File(mainBranchPath + "/mode.conf")
    assert(mainBranchModeConfFile.exists())
    val mainBranchModeConfig: Config = ConfigFactory.parseFile(mainBranchModeConfFile)
    assert(mainBranchModeConfig.getString("mode") == "onewrite")

    val mainBranchStatusConfFile = new File(mainBranchPath + "/status.conf")
    assert(mainBranchStatusConfFile.exists())
    val mainBranchStatusConfig: Config = ConfigFactory.parseFile(mainBranchStatusConfFile)
    assert(mainBranchStatusConfig.getString("status") == "init")

    val mainBranchVersionConfFile = new File(mainBranchPath + "/lastversion.conf")
    assert(mainBranchVersionConfFile.exists())
    val mainBranchVersionConfig: Config = ConfigFactory.parseFile(mainBranchVersionConfFile)
    assert(mainBranchVersionConfig.getString("lastversion") == "1")

    val mainBranchVersionsConfFile = new File(mainBranchPath + "/versions.conf")
    assert(mainBranchVersionsConfFile.exists())
    val mainBranchVersionsConfig: Config = ConfigFactory.parseFile(mainBranchVersionsConfFile)
    assert(mainBranchVersionsConfig.getStringList("versions").contains("1"))

    val version1Path = mainBranchPath + "/1"
    val version1Dir = new File(version1Path)
    assert(version1Dir.exists())
    assert(version1Dir.isDirectory)

    val modelBranchesFile = new File(modelPath + "/branches.conf")
    assert(modelBranchesFile.exists())
    val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
    assert(modelBranchesConfig.getStringList("branches").contains("main"))
    val expected = """[
                     |{"name":"testcsvmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testcsvmodel","workMessage":"Initialization is successful"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("ATest 2. Create new model with user defined files format (json)") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testjsonmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (modelDir.exists()) {
      assert(modelDir.exists())
      assert(modelDir.isDirectory)
    }
    val simpleQuery = SimpleQuery("model=testjsonmodel format=json")
    val initCommand = new FSInit(simpleQuery, utils)
    val actual = execute(initCommand)
    assert(modelDir.exists())
    assert(modelDir.isDirectory)

    val modelConfFile = new File(modelPath + "/model.conf")
    assert(modelConfFile.exists())
    val modelConfig: Config = ConfigFactory.parseFile(modelConfFile)
    assert(modelConfig.getString("model") == "testjsonmodel")

    val formatConfFile = new File(modelPath + "/format.conf")
    assert(formatConfFile.exists())
    val formatConfig: Config = ConfigFactory.parseFile(formatConfFile)
    assert(formatConfig.getString("format") == "json")

    val branchesConfFile = new File(modelPath + "/branches.conf")
    assert(branchesConfFile.exists())
    val branchesConfig: Config = ConfigFactory.parseFile(branchesConfFile)
    assert(branchesConfig.getStringList("branches").contains("main"))

    val mainBranchPath = modelPath + "/main"
    val mainBranchDir = new File(mainBranchPath)
    assert(mainBranchDir.exists())
    assert(mainBranchDir.isDirectory)

    val mainBranchNameConfFile = new File(mainBranchPath + "/name.conf")
    assert(mainBranchNameConfFile.exists())
    val mainBranchNameConfig: Config = ConfigFactory.parseFile(mainBranchNameConfFile)
    assert(mainBranchNameConfig.getString("name") == "main")

    val mainBranchModeConfFile = new File(mainBranchPath + "/mode.conf")
    assert(mainBranchModeConfFile.exists())
    val mainBranchModeConfig: Config = ConfigFactory.parseFile(mainBranchModeConfFile)
    assert(mainBranchModeConfig.getString("mode") == "onewrite")

    val mainBranchStatusConfFile = new File(mainBranchPath + "/status.conf")
    assert(mainBranchStatusConfFile.exists())
    val mainBranchStatusConfig: Config = ConfigFactory.parseFile(mainBranchStatusConfFile)
    assert(mainBranchStatusConfig.getString("status") == "init")

    val mainBranchVersionConfFile = new File(mainBranchPath + "/lastversion.conf")
    assert(mainBranchVersionConfFile.exists())
    val mainBranchVersionConfig: Config = ConfigFactory.parseFile(mainBranchVersionConfFile)
    assert(mainBranchVersionConfig.getString("lastversion") == "1")

    val mainBranchVersionsConfFile = new File(mainBranchPath + "/versions.conf")
    assert(mainBranchVersionsConfFile.exists())
    val mainBranchVersionsConfig: Config = ConfigFactory.parseFile(mainBranchVersionsConfFile)
    assert(mainBranchVersionsConfig.getStringList("versions").contains("1"))

    val version1Path = mainBranchPath + "/1"
    val version1Dir = new File(version1Path)
    assert(version1Dir.exists())
    assert(version1Dir.isDirectory)

    val modelBranchesFile = new File(modelPath + "/branches.conf")
    assert(modelBranchesFile.exists())
    val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
    assert(modelBranchesConfig.getStringList("branches").contains("main"))
    val expected = """[
                     |{"name":"testjsonmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testjsonmodel","workMessage":"Initialization is successful"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
