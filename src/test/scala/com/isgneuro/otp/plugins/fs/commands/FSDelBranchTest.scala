package com.isgneuro.otp.plugins.fs.commands

import com.typesafe.config.{Config, ConfigFactory}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File

class FSDelBranchTest extends CommandTest{
  val dataset: String = ""

  test("Delete branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val branchPath = modelPath + "/branch6"
      val branchDir = new File(branchPath)
      if (!branchDir.exists()) {
        log.error("Branch branch6 doesn't exists in model testmodel.")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=branch6""")
        val commandWriteFile = new FSDelBranch(simpleQuery, utils)
        val actual = execute(commandWriteFile)
        assert(!branchDir.exists())

        val branch2ChildsFile = new File(branchPath + "/childbranches.conf")
        assert(branch2ChildsFile.exists())
        val branch2ChildsConfig: Config = ConfigFactory.parseFile(branch2ChildsFile)
        assert(!branch2ChildsConfig.getStringList("childbranches").contains("branch6"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(!modelBranchesConfig.getStringList("branches").contains("branch6"))
        val expected =
          """[
            |{"name":"branch6","model":"testmodel","workMessage":"Deleting is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("Delete branch with many childs and deeping of childs") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    val modelDir = new File(modelPath)
    if (!modelDir.exists()) {
      log.error("Model testmodel doesn't exists.")
    } else {
      val branchPath = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch1"
      val branchDir = new File(branchPath)
      val branchc1Path = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch3"
      val branchc1Dir = new File(branchc1Path)
      val branchc2Path = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch4"
      val branchc2Dir = new File(branchc2Path)
      if (!branchDir.exists()) {
        log.error("Branch branch1 doesn't exists in model testmodel.")
      } else if (!branchc1Dir.exists()) {
        log.error("Branch branch3 doesn't exists in model testmodel.")
      } else if (!branchc2Dir.exists()) {
        log.error("Branch branch4 doesn't exists in model testmodel.")
      } else {
        val simpleQuery = SimpleQuery("""model=testmodel branch=branch1""")
        val commandWriteFile = new FSDelBranch(simpleQuery, utils)
        val actual = execute(commandWriteFile)
        assert(!branchDir.exists())
        assert(!branchc1Dir.exists())
        assert(!branchc2Dir.exists())

        val mainBranchChildsFile = new File(modelPath + "/main/childbranches.conf")
        assert(mainBranchChildsFile.exists())
        val mainBranchChildsConfig: Config = ConfigFactory.parseFile(mainBranchChildsFile)
        assert(!mainBranchChildsConfig.getStringList("childbranches").contains("branch1"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(!modelBranchesConfig.getStringList("branches").contains("branch1") && !modelBranchesConfig.getStringList("branches").contains("branch3")
          && !modelBranchesConfig.getStringList("branches").contains("branch4"))
        val expected =
          """[
            |{"name":"branch3","model":"testmodel","workMessage":"Deleting is successful"},
            |{"name":"branch4","model":"testmodel","workMessage":"Deleting is successful"},
            |{"name":"branch1","model":"testmodel","workMessage":"Deleting is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }
}
