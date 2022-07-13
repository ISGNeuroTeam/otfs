package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FSGetChildBranchesTest extends CommandTest {

  val dataset: String = ""

  test ("Get branches with defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery(
        "model=testmodel branch=branch1 showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=true")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 8)
      assert(actualSet.count() == 2)
      assert(actual.contains(""""name":"branch3""""))
      assert(actual.contains(""""name":"branch4""""))
    }
  }

  test ("Get branches with default command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=main")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 2)
      assert(actualSet.count() == 2)
      assert(actual.contains(""""name":"branch1""""))
      assert(actual.contains(""""name":"branch2""""))
    }
  }

  test ("Get branches with partially defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=branch2 showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=false")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 5)
      assert(actualSet.count() == 2)
      assert(actual.contains(""""name":"branch5""""))
      assert(actual.contains(""""name":"branch6""""))
    }
  }

  test ("Get only empty branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=main showlastupdatedate=true showlastversionnum=true onlyempty=true")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 0)
      assert(actualSet.count() == 0)
    }
  }

  test ("Get only not empty branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=main showlastupdatedate=true showlastversionnum=true onlynonempty=true")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 4)
      assert(actualSet.count() == 2)
    }
  }

  test("Get only with child branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=main showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 4)
      assert(actualSet.count() == 2)
    }
  }

  test("Get only without child branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=branch2 showlastupdatedate=true showlastversionnum=true onlywithoutchildbranches=true")
      val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.columns.length == 4)
      assert(actualSet.count() == 2)
    }
  }

}
