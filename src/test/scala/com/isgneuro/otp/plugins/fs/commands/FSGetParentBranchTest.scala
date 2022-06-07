package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSGetParentBranchTest extends CommandTest {

  val dataset: String = ""

  test("Get parent branch info with defined command param values") {
    val simpleQuery = SimpleQuery(
      "model=electronic branch=resistors showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true showversionslist=true")
    val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get parent branch info with default command param values") {
    val simpleQuery = SimpleQuery("model=electronic branch=resistors")
    val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get parent branch info with partially defined command param values") {
    val simpleQuery = SimpleQuery("model=electronic branch=variants showdataexistsinfo=true showcreationdate=true showversionslist=true")
    val commandWriteFile = new FSGetParentBranch(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
