package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSDelModelTest extends CommandTest {
  val dataset: String = ""

  test("Delete model") {
    val simpleQuery = SimpleQuery("""model=tempModel""")
    val commandWriteFile = new FSDelModel(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
