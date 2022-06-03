package com.isgneuro.otp.plugins.fs.commands

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FsInitTest extends CommandTest {
  val dataset: String = ""

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  test("Test 0. Create new model with default files format") {
    val simpleQuery = SimpleQuery("model=tempModel")
    val initCommand = new FSInit(simpleQuery, utils)
    val actual = execute(initCommand)
    val modelPath = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic"
    val modelDir = new File(modelPath)
    assert(modelDir.exists())
    val modelConfFile = new File(modelPath + "/model.conf")
    assert(modelConfFile.exists())
    val modelConfig: Config = ConfigFactory.parseFile(modelConfFile)
    assert(modelConfig.getString("model") == "electronic")
    assert(modelConfig.getString("format") == "parquet")
    //assert(modelConfig.getStringList("branches") == Array[String]("main").toIterable.asJava)
    val mainBranchPath = modelPath + "/main"
    val mainBranchDir = new File(mainBranchPath)
    assert(mainBranchDir.exists())
    val mainBranchConfFile = new File(mainBranchPath + "/branch.conf")
    assert(mainBranchConfFile.exists())
    val mainBranchConfig: Config = ConfigFactory.parseFile(mainBranchConfFile)
    assert(mainBranchConfig.getString("branchname") == "main")
    assert(mainBranchConfig.getString("mode") == "onewrite")
    assert(mainBranchConfig.getString("lastversion") == "1")
    val version1Path = mainBranchPath + "/1"
    val version1Dir = new File(version1Path)
    assert(version1Dir.exists())
    val expected = """[
                     |{"name":"electronic","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic","workMessage":"Initialization is succcesfull"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Test 1. Create new model with user defined files format") {
    val simpleQuery = SimpleQuery("model=economic format=csv")
    val initCommand = new FSInit(simpleQuery, utils)
    val actual = execute(initCommand)
    val modelPath = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/economic"
    val modelDir = new File(modelPath)
    assert(modelDir.exists())
    val modelConfFile = new File(modelPath + "/model.conf")
    assert(modelConfFile.exists())
    val modelConfig: Config = ConfigFactory.parseFile(modelConfFile)
    assert(modelConfig.getString("model") == "economic")
    assert(modelConfig.getString("format") == "csv")
    //assert(modelConfig.getStringList("branches") == Array[String]("main").toIterable.asJava)
    val mainBranchPath = modelPath + "/main"
    val mainBranchDir = new File(mainBranchPath)
    assert(mainBranchDir.exists())
    val mainBranchConfFile = new File(mainBranchPath + "/branch.conf")
    assert(mainBranchConfFile.exists())
    val mainBranchConfig: Config = ConfigFactory.parseFile(mainBranchConfFile)
    assert(mainBranchConfig.getString("branchname") == "main")
    assert(mainBranchConfig.getString("mode") == "onewrite")
    assert(mainBranchConfig.getString("lastversion") == "1")
    val version1Path = mainBranchPath + "/1"
    val version1Dir = new File(version1Path)
    assert(version1Dir.exists())
    val expected = """[
                     |{"name":"economic","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/economic","workMessage":"Initialization is succcesfull"}
                     |]""".stripMargin
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }
}
