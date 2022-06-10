package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSDelBranchTest extends CommandTest{
  val dataset: String = ""

  test("Delete branch") {
    val simpleQuery = SimpleQuery("""model=tempModel branch=testing""")
    val commandWriteFile = new FSDelBranch(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
