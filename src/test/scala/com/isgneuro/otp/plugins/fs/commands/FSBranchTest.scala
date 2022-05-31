package com.isgneuro.otp.plugins.fs.commands

import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class FSBranchTest extends CommandTest{
  val dataset: String = ""

  test("branch from main by default") {
    val simpleQuery = SimpleQuery("name=resistors model=electronic")
    val branchCommand = new FSBranch(simpleQuery, utils)
    execute(branchCommand)
  }

  test("branch from main") {
    val simpleQuery = SimpleQuery("name=dividends model=economic from=main")
    val branchCommand = new FSBranch(simpleQuery, utils)
    execute(branchCommand)
  }

  test("branch from some branch") {
    val simpleQuery = SimpleQuery("name=variants model=electronic from=resistors")
    val branchCommand = new FSBranch(simpleQuery, utils)
    execute(branchCommand)
  }

  test("branch from unknown branch") {
    val simpleQuery = SimpleQuery("name=promotions model=economic from=superbranch")
    val branchCommand = new FSBranch(simpleQuery, utils)
    execute(branchCommand)
  }
}
