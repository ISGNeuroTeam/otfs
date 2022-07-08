package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSGetBranchesTest extends CommandTest {

  val dataset: String = ""

  test ("Get branches with defined command param values") {

    val simpleQuery = SimpleQuery(
      "model=testmodel showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get branches with default command param values") {
    val simpleQuery = SimpleQuery("model=testmodel")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get branches with partially defined command param values") {
    val simpleQuery = SimpleQuery("model=testmodel showcreationdate=false showlastupdatedate=true showlastversionnum=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get only empty branches") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlyempty=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Get only not empty branches") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlynonempty=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test ("Onlyempty and onlynonempty params - true") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlyempty=true onlynonempty=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get only with child branches") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Get only without child branches") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlywithoutchildbranches=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Onlywithchildbranches and onlywithoutchildbranches params - true") {
    val simpleQuery = SimpleQuery("model=testmodel showlastupdatedate=true showlastversionnum=true onlywithchildbranches=true onlywithoutchildbranches=true")
    val commandWriteFile = new FSGetBranches(simpleQuery, utils)
    execute(commandWriteFile)
  }
}
