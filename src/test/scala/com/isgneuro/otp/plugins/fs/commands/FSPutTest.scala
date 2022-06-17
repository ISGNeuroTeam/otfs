package com.isgneuro.otp.plugins.fs.commands

import com.isgneuro.otp.plugins.fs.config.BranchConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import ot.dispatcher.sdk.test.CommandTest
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSPutTest extends CommandTest {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  override val dataset: String = """[
                                   |{"a":"1","b":"2"},
                                   |{"a":"10","b":"20"}
                                   |]""".stripMargin

  val datasetToAppend: String = """[
                                  |{"a":"100","b":"200"}
                                  |]""".stripMargin

  val dataset3cols: String = """[
                               |{"a":"1","b":"2","c":"3"},
                               |{"a":"10","b":"2","c":"30"},
                               |{"a":"10","b":"20","c":"300"}
                               |]""".stripMargin

  val appended: String = """[
                           | {"a":"1","b":"2"},
                           |{"a":"10","b":"20"},
                           |{"a":"100","b":"200"}
                           |]""".stripMargin

  val datasetNulls: String = """[
                               | {"a":"1","b":"2","c":null},
                               |{"a":"10","b":"20","c":null},
                               |{"a":"100","b":"200","c":null}
                               |]""".stripMargin

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  test("Write parquet") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=parquet path=write_test_file_parquet """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("parquet").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Write to main by default") {
    val simpleQuery = SimpleQuery("""model=tempModel format=parquet""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to main") {
    val simpleQuery = SimpleQuery("""model=tempModel branch=main format=parquet""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to main newVersion=true"){
    val simpleQuery = SimpleQuery("""model=electronic branch=main format=parquet newversion=true""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to main newVersion=false"){
    val simpleQuery = SimpleQuery("""model=electronic branch=main format=parquet newversion=false""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to unknown branch") {
    val simpleQuery = SimpleQuery("""model=electronic branch=somebranch format=parquet""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to some branch newVersion=false") {
    val simpleQuery = SimpleQuery("""model=tempModel2 branch=expo newversion=false""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write to some branch newVersion=true") {
    val simpleQuery = SimpleQuery("""model=tempModel2 branch=expo newversion=true""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Write parquet to default branch when branch is init") {
    val branchConfig = new BranchConfig("file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic", "main")
    val status = branchConfig.getStatus().getOrElse("")
    assert(status == "init")
    val lastVersion = branchConfig.getLastVersion().getOrElse("1")
    val path = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic/main/" + lastVersion
    val simpleQuery = SimpleQuery("""model=electronic format=parquet""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    assert(new File(path).exists())
    val actual = spark.read.format("parquet").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  //test write to default branch when status hasData, need checking assert status=hasData from config
  test("Write parquet to default branch when branch already has data") {
    val branchConfig = new BranchConfig("file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic", "main")
    val status = branchConfig.getStatus().getOrElse("")
    assert(status == "hasData")
    val lastVersion = branchConfig.getLastVersion().getOrElse("1")
    val path = "file:///home/rkpvteh/src/otfs/src/test/resources/temp/electronic/main/" + (lastVersion + 1)
    val simpleQuery = SimpleQuery("""model=electronic format=parquet""")
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    assert(new File(path).exists())
    val actual = spark.read.format("parquet").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Write parquet to not created branch") {
    val simpleQuery = SimpleQuery("""model=electronic format=parquet branch=""")
  }

  test("Write json") {
    val path = new File("src/test/resources/temp/write_test_file_json").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=json path=write_test_file_json """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("json").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Write csv") {
    val path = new File("src/test/resources/temp/write_test_file_csv").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=csv path=write_test_file_csv """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    val actual = spark.read.format("csv").option("header", "true").load(path).toJSON.collect().mkString("[\n",",\n","\n]")
    val expected = dataset
    assert(jsonCompare(actual, expected), f"Result : $actual\n---\nExpected : $expected")
  }

  test("Write parquet + partitionBy") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" partitionBy=a path=write_test_file_parquet """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(commandWriteFile)
    val expected = jsonToDf(dataset)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b").sort("a")
    assert(actualDF.rdd.getNumPartitions == 2)
    assert(actualDF.except(expected).count() == 0)
  }

  test("Write parquet + partitionBy on multiple columns") {
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" partitionBy=a,b path=write_test_file_parquet """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    execute(jsonToDf(dataset3cols), commandWriteFile)
    val expected = jsonToDf(dataset3cols)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b", "c").sort("a")
    assert(actualDF.rdd.getNumPartitions == 3)
    assert(actualDF.except(expected).count() == 0)
  }

  test("Write with different modes") { // TODO Make one test per mode
    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath

    execute(jsonToDf(datasetToAppend), new FSPut(SimpleQuery(""" path=write_test_file_parquet """), utils))
    execute(jsonToDf(dataset), new FSPut(SimpleQuery(""" path=write_test_file_parquet """), utils))
    execute(jsonToDf(datasetToAppend), new FSPut(SimpleQuery(""" path=write_test_file_parquet mode=append """), utils))

    val expected = jsonToDf(appended)
    val actualDF = spark.read.format("parquet").load(path).select("a", "b").sort("a")
    assert(actualDF.except(expected).count() == 0)

    execute(jsonToDf(dataset), new FSPut(SimpleQuery(""" path=write_test_file_parquet mode=ignore """), utils))
    val actualDFignored = spark.read.format("parquet").load(path).select("a", "b").sort("a")
    assert(actualDFignored.except(expected).count() == 0)
  }

  test("Throw an error if no path specified") {
    val simpleQuery = SimpleQuery(""" format=parquet """)
    val thrown = intercept[Exception] {
      val commandWriteFile = new FSPut(simpleQuery, utils)
      execute(commandWriteFile)
    }
    assert(thrown.getMessage.contains("No path specified"))
  }

  test("Write null column") {
    val df = Seq(
      (1, "qwe"),
      (2, "rty")
    ).toDF("a", "b").withColumn("c", lit(null))

    val path = new File("src/test/resources/temp/write_test_file_parquet").getAbsolutePath
    val simpleQuery = SimpleQuery(""" format=parquet path=write_test_file_parquet """)
    val commandWriteFile = new FSPut(simpleQuery, utils)
    val _ = commandWriteFile.transform(df)

    val actualReaded = spark.read.format("parquet").load(path)
    assert(actualReaded.columns.sameElements(Array("a", "b", "c")))
    assert(actualReaded.except(df).count() == 0)
  }
}