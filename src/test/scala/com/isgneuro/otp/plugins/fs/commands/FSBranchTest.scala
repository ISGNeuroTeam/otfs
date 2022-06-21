package com.isgneuro.otp.plugins.fs.commands

import com.typesafe.config.{Config, ConfigFactory}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import java.io.File
import scala.collection.JavaConverters.asJavaIterableConverter

class FSBranchTest extends CommandTest{
  val dataset: String = ""

  test("branch from main by default") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch1"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch1 model=testmodel")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch1")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "main")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val mainBranchChildsFile = new File(modelPath + "/main/childbranches.conf")
        assert(mainBranchChildsFile.exists())
        val mainBranchChildsConfig: Config = ConfigFactory.parseFile(mainBranchChildsFile)
        assert(mainBranchChildsConfig.getStringList("childbranches").contains("branch1"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        val expected =
          """[
            |{"name":"branch1","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch1","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("branch from main") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch2"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch2 model=testmodel from=main")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch2")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "main")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val mainBranchChildsFile = new File(modelPath + "/main/childbranches.conf")
        assert(mainBranchChildsFile.exists())
        val mainBranchChildsConfig: Config = ConfigFactory.parseFile(mainBranchChildsFile)
        assert(mainBranchChildsConfig.getStringList("childbranches").contains("branch1"))
        assert(mainBranchChildsConfig.getStringList("childbranches").contains("branch2"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch2"))
        val expected =
          """[
            |{"name":"branch2","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch2","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("branch from some branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch3"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch3 model=testmodel from=branch1")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch3")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "branch1")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val branch1ChildsFile = new File(modelPath + "/branch1/childbranches.conf")
        assert(branch1ChildsFile.exists())
        val branch1ChildsConfig: Config = ConfigFactory.parseFile(branch1ChildsFile)
        assert(branch1ChildsConfig.getStringList("childbranches").contains("branch3"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch2"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch3"))
        val expected =
          """[
            |{"name":"branch3","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch3","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("additional branch from some branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch4"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch4 model=testmodel from=branch1")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch4")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "branch1")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val branch1ChildsFile = new File(modelPath + "/branch1/childbranches.conf")
        assert(branch1ChildsFile.exists())
        val branch1ChildsConfig: Config = ConfigFactory.parseFile(branch1ChildsFile)
        assert(branch1ChildsConfig.getStringList("childbranches").contains("branch3"))
        assert(branch1ChildsConfig.getStringList("childbranches").contains("branch4"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch2"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch3"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch4"))
        val expected =
          """[
            |{"name":"branch4","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch4","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("branch from another branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch5"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch5 model=testmodel from=branch2")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch5")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "branch2")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val branch2ChildsFile = new File(modelPath + "/branch2/childbranches.conf")
        assert(branch2ChildsFile.exists())
        val branch2ChildsConfig: Config = ConfigFactory.parseFile(branch2ChildsFile)
        assert(branch2ChildsConfig.getStringList("childbranches").contains("branch5"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch2"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch3"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch4"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch5"))
        val expected =
          """[
            |{"name":"branch5","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch5","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("additional branch from another branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val branchPath = modelPath + "/branch6"
      val branchDir = new File(branchPath)
      if (branchDir.exists()) {
        assert(branchDir.exists())
        assert(branchDir.isDirectory)
      } else {
        val simpleQuery = SimpleQuery("name=branch6 model=testmodel from=branch2")
        val branchCommand = new FSBranch(simpleQuery, utils)
        val actual = execute(branchCommand)
        assert(branchDir.exists())
        assert(branchDir.isDirectory)

        val branchNameConfFile = new File(branchPath + "/name.conf")
        assert(branchNameConfFile.exists())
        val branchNameConfig: Config = ConfigFactory.parseFile(branchNameConfFile)
        assert(branchNameConfig.getString("name") == "branch6")

        val branchStatusConfFile = new File(branchPath + "/status.conf")
        assert(branchStatusConfFile.exists())
        val branchStatusConfig: Config = ConfigFactory.parseFile(branchStatusConfFile)
        assert(branchStatusConfig.getString("status") == "init")

        val branchVersionConfFile = new File(branchPath + "/lastversion.conf")
        assert(branchVersionConfFile.exists())
        val branchVersionConfig: Config = ConfigFactory.parseFile(branchVersionConfFile)
        assert(branchVersionConfig.getString("lastversion") == "1")

        val branchVersionsConfFile = new File(branchPath + "/versions.conf")
        assert(branchVersionsConfFile.exists())
        val branchVersionsConfig: Config = ConfigFactory.parseFile(branchVersionsConfFile)
        assert(branchVersionsConfig.getStringList("versions").contains("1"))

        val branchParentConfFile = new File(branchPath + "/parentbranch.conf")
        assert(branchParentConfFile.exists())
        val branchParentConfig: Config = ConfigFactory.parseFile(branchParentConfFile)
        assert(branchParentConfig.getString("parentbranch") == "branch2")

        val version1Path = branchPath + "/1"
        val version1Dir = new File(version1Path)
        assert(version1Dir.exists())
        assert(version1Dir.isDirectory)

        val branch2ChildsFile = new File(modelPath + "/branch2/childbranches.conf")
        assert(branch2ChildsFile.exists())
        val branch2ChildsConfig: Config = ConfigFactory.parseFile(branch2ChildsFile)
        assert(branch2ChildsConfig.getStringList("childbranches").contains("branch5"))
        assert(branch2ChildsConfig.getStringList("childbranches").contains("branch6"))

        val modelBranchesFile = new File(modelPath + "/branches.conf")
        assert(modelBranchesFile.exists())
        val modelBranchesConfig: Config = ConfigFactory.parseFile(modelBranchesFile)
        assert(modelBranchesConfig.getStringList("branches").contains("main"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch1"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch2"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch3"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch4"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch5"))
        assert(modelBranchesConfig.getStringList("branches").contains("branch6"))
        val expected =
          """[
            |{"name":"branch6","model":"testmodel","path":"file:///home/rkpvteh/src/otfs/src/test/resources/temp/testmodel/branch6","workMessage":"Initialization is successful"}
            |]""".stripMargin
        assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
      }
    }
  }

  test("branch from unknown branch") {
    val modelPath = utils.pluginConfig.getString("storage.fs") + new File(utils.pluginConfig.getString("storage.path"), "testmodel" + "/").getAbsolutePath
    if (!new File(modelPath).exists()) {
      log.error("Model testmodel doesn't exists")
    } else {
      val simpleQuery = SimpleQuery("name=newbranch model=testmodel from=superbranch")
      val thrown = intercept[Exception] {
        val branchCommand = new FSBranch(simpleQuery, utils)
        execute(branchCommand)
      }
      assert(thrown.getMessage.contains("Branch superbranch doesn't exists in model testmodel. Use command fsbranch for new branch creation"))
    }
  }
}
