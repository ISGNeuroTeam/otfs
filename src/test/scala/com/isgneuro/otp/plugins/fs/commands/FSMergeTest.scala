package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSMergeTest extends CommandTest{
  val dataset: String = ""

  test("Merge") {
    val simpleQuery = SimpleQuery("""model=electronic outbranch=resistors inbranch=main""")
    val commandWriteFile = new FSMerge(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
