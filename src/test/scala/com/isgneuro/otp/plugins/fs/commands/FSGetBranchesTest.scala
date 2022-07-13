package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FSGetBranchesTest extends CommandTest {

  val dataset: String = ""

  test ("Get branches with defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery(
        """model=testmodel showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 7)
      assert(actualSet.columns.length == 8)
    }
  }

  test ("Get branches with default command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      assert(jsonToDf(actual).columns.length == 2)
    }
  }

  test ("Get branches with partially defined command param values") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showcreationdate=false showlastupdatedate=true showlastversionnum=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      assert(jsonToDf(actual).columns.length == 4)
    }
  }

  test ("Get only empty branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlyempty=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 2)
      assert(actualSet.columns.length == 4)
      assert(actual.contains(""""name":"branch5""""))
      assert(actual.contains(""""name":"branch6""""))
    }
  }

  test ("Get only not empty branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlynonempty=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 5)
      assert(actualSet.columns.length == 4)
      assert(actual.contains(""""name":"main""""))
      assert(actual.contains(""""name":"branch1""""))
      assert(actual.contains(""""name":"branch2""""))
      assert(actual.contains(""""name":"branch3""""))
      assert(actual.contains(""""name":"branch4""""))
    }
  }

  test ("Onlyempty and onlynonempty params - true") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlyempty=true onlynonempty=true""")
      val thrown = intercept[Exception] {
        val commandWriteFile = new FSGetBranches(simpleQuery, utils)
        execute(commandWriteFile)
      }
      assert(thrown.getMessage.contains("Error: equal values in opposite parameters onlyempty and onlynonempty. Define various values in these params or define only one param."))
    }
  }

  test("Get only with child branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 3)
      assert(actualSet.columns.length == 4)
      assert(actual.contains(""""name":"main""""))
      assert(actual.contains(""""name":"branch1""""))
      assert(actual.contains(""""name":"branch2""""))
    }
  }

  test("Get only without child branches") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlywithoutchildbranches=true""")
      val commandWriteFile = new FSGetBranches(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val actualSet = jsonToDf(actual)
      assert(actualSet.count() == 4)
      assert(actualSet.columns.length == 4)
      assert(actual.contains(""""name":"branch3""""))
      assert(actual.contains(""""name":"branch4""""))
      assert(actual.contains(""""name":"branch5""""))
      assert(actual.contains(""""name":"branch6""""))
    }
  }

  test("Onlywithchildbranches and onlywithoutchildbranches params - true") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getPath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testmodel showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true onlywithoutchildbranches=true""")
      val thrown = intercept[Exception] {
        val commandWriteFile = new FSGetBranches(simpleQuery, utils)
        execute(commandWriteFile)
      }
      assert(thrown.getMessage.contains("Error: equal values in opposite parameters onlyWithChildBranches and onlyWithoutChildBranches. Define various values in these params or define only one param."))
    }
  }
}
