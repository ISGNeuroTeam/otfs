package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSGetChildBranchesTest extends CommandTest {

  val dataset: String = ""

  test ("Get branches with defined command param values") {
    val simpleQuery = SimpleQuery(
      "model=tempModel branch=transistors showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=true")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get branches with default command param values") {
    val simpleQuery = SimpleQuery("model=tempModel branch=main")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get branches with partially defined command param values") {
    val simpleQuery = SimpleQuery("model=tempModel branch=resistors showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=false")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get only empty branches") {
    val simpleQuery = SimpleQuery("model=electronic branch=main showlastupdatedate=true showlastversionnum=true onlyempty=true")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get only not empty branches") {
    val simpleQuery = SimpleQuery("model=electronic branch=main showlastupdatedate=true showlastversionnum=true onlynonempty=true")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get only with child branches") {
    val simpleQuery = SimpleQuery("model=tempModel branch=main showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get only without child branches") {
    val simpleQuery = SimpleQuery("model=tempModel branch=main showlastupdatedate=true showlastversionnum=true onlywithoutchildbranches=true")
    val commandWriteFile = new FSGetChildBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

}
