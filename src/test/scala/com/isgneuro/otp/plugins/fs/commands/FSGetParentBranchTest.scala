package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FSGetParentBranchTest extends CommandTest {

  val dataset: String = ""

  test("Get parent branch info with defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery(
        "model=testmodel branch=branch1 showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true showversionslist=true")
      val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 1)
      assert(actualSet.columns.length == 7)
      assert(actual.contains(""""name":"main""""))
    }
  }

  test("Get parent branch info with default command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=branch4")
      val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 1)
      assert(actualSet.columns.length == 2)
      assert(actual.contains(""""name":"branch1""""))
    }
  }

  test("Get parent branch info with partially defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("model=testmodel branch=branch5 showdataexistsinfo=true showcreationdate=true showversionslist=true")
      val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 1)
      assert(actualSet.columns.length == 5)
      assert(actual.contains(""""name":"branch2""""))
    }
  }
}
