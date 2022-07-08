package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class _7FSDelModelTest extends CommandTest {
  val dataset: String = ""

  test("Delete model") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testcsvmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testcsvmodel doesn't exists.")
    } else {
      val simpleQuery = SimpleQuery("""model=testcsvmodel""")
      val commandWriteFile = new FSDelModel(simpleQuery, utils)
      val actual = execute(commandWriteFile)
      val expected =
          """[
            |{"name":"testcsvmodel","workMessage":"Deleting is successful"}
            |]""".stripMargin
      assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
    }
  }
}
