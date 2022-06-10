package com.isgneuro.otp.plugins.fs.commands

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.test.CommandTest
import ot.dispatcher.sdk.core.SimpleQuery

import java.io.File

class FSGetTest extends CommandTest {
  Logger
    .getLogger("org")
    .setLevel(Level.ERROR)

  override val dataset: String = """[
                                   |{"a":"1","b":"2"},
                                   |{"a":"10","b":"20"}
                                   |]""".stripMargin

  val initialDf: DataFrame = jsonToDf(dataset)

  private lazy val sparkSession: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

  import sparkSession.implicits._

  private val df: DataFrame = Seq(
    (1, "qwe"),
    (10, "rty")
  ).toDF("a", "b")

  test ("Read files by default") {
    val simpleQuery = SimpleQuery("""model=tempModel""")
    val commandWriteFile = new FSGet(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Read files with defined branch") {
    val simpleQuery = SimpleQuery("""model=tempModel branch=resistors""")
    val commandWriteFile = new FSGet(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Read files with defined version") {
    val simpleQuery = SimpleQuery("""model=tempModel version=1""")
    val commandWriteFile = new FSGet(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Read files with defined branch and version") {
    val simpleQuery = SimpleQuery("""model=tempModel branch=resistors version=1""")
    val commandWriteFile = new FSGet(simpleQuery, utils)
    execute(commandWriteFile)
  }

  test("Read files in parquet format") {
    df.show()
    val path = new File("src/test/resources/temp/read_test_file_parquet").getAbsolutePath
    df.write.format("parquet").mode("overwrite").save(path)
    val simpleQuery = SimpleQuery(""" format=parquet path=read_test_file_parquet """)
    val commandReadFile = new FSGet(simpleQuery, utils)
    val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
    val expectedCols = Array("a", "b")
    assert(actual.columns.sameElements(expectedCols))
    assert(actual.except(df).count() == 0)
  }

  test("Set default format as parquet if no format specified") {
    df.show()
    val path = new File("src/test/resources/temp/read_test_file_parquet").getAbsolutePath
    df.write.format("parquet").mode("overwrite").save(path)
    val simpleQuery = SimpleQuery(""" path=read_test_file_parquet """)
    val commandReadFile = new FSGet(simpleQuery, utils)
    val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
    val expectedCols = Array("a", "b")

    assert(actual.columns.sameElements(expectedCols))
    assert(actual.except(df).count() == 0)
  }

  test("Infer schema if 'csv' format specified ") {
    df.show()
    val path = new File("src/test/resources/temp/read_test_file_csv").getAbsolutePath
    df.write.format("csv").option("header", "true").mode("overwrite").save(path)
    val simpleQuery = SimpleQuery(""" format=csv path=read_test_file_csv """)
    val commandReadFile = new FSGet(simpleQuery, utils)
    val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
    val expectedSchema = StructType(
      List(
        StructField("a", IntegerType),
        StructField("b", StringType)
      )
    )

    assert(actual.columns.sameElements(expectedSchema.map(_.name)))
    assert(actual.schema.equals(expectedSchema))
    assert(actual.except(df).count() == 0)
  }

  test("Do not infer schema if 'csv' format specified and inferSchema=false") {
    df.show()
    val path = new File("src/test/resources/temp/read_test_file_csv").getAbsolutePath
    df.write.format("csv").option("header", "true").mode("overwrite").save(path)
    val simpleQuery = SimpleQuery(""" format=csv path=read_test_file_csv inferSchema=false""")
    val commandReadFile = new FSGet(simpleQuery, utils)
    val actual = commandReadFile.transform(sparkSession.emptyDataFrame)
    val expectedSchema = StructType(
      List(
        StructField("a", StringType),
        StructField("b", StringType)
      )
    )

    assert(actual.columns.sameElements(expectedSchema.map(_.name)))
    assert(actual.schema.equals(expectedSchema))
    assert(actual.except(df).count() == 0)
  }

  test("Throw an error if no path specified") {
    df.show()
    val path = new File("src/test/resources/temp/read_test_file_parquet").getAbsolutePath
    df.write.format("parquet").mode("overwrite").save(path)
    val simpleQuery = SimpleQuery(""" format=parquet """)
    val thrown = intercept[Exception] {
      val commandReadFile = new FSGet(simpleQuery, utils)
      execute(commandReadFile)
    }
    assert(thrown.getMessage.startsWith("No path specified"))
  }
}