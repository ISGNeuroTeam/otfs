package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSMergeTest extends CommandTest{
  val dataset: String = ""

  test("Merge") {
    val simpleQuery = SimpleQuery("""model=tempModel outbranch=resistors inbranch=main""")
    val commandWriteFile = new FSMerge(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Merge with outbranchversion defining") {
    val simpleQuery = SimpleQuery("""model=electronic outbranch=resistors inbranch=main outbranchversion=1""")
    val commandWriteFile = new FSMerge(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Merge with inbranchversion defining") {
    val simpleQuery = SimpleQuery("""model=electronic outbranch=resistors inbranch=main inbranchversion=1""")
    val commandWriteFile = new FSMerge(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Merge with out- and inbranchversion defining") {
    val simpleQuery = SimpleQuery("""model=electronic outbranch=resistors inbranch=main outbranchversion=1 inbranchversion=1""")
    val commandWriteFile = new FSMerge(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
