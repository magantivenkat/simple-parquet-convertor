/*
 * Copyright 2018-2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package africa.absa.parquet.convertor

import scala.io.Source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite

case class Person(first: String, last: String, street: String, city: String, age: Int, employed: Boolean)

class SimpleParquetConvertorSuite extends FunSuite {

  val spark = SparkSession.builder().appName("SimpleParquetConvertorSuite").master("local[2]").getOrCreate()
  import spark.implicits._

  val peopleDataCount = 20
  val loremDataCount = 19
  val peopleDataSchemaTyped = DataType.fromJson(Source.fromFile("src/test/resources/data/peopleDataSchemaTyped.json")
    .getLines().mkString("\n")).asInstanceOf[StructType]

  test("Convert CSV with header to parquet - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleCSVParquetSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/peopleHeader.csv"
    SimpleParquetConvertor.main(Array(
      "--source-format", "csv",
      "--target-format", "parquet",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.parquet(testOutputDir)

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.option("header", true).option("comment", "#").option("inferSchema", true).csv(srcPath)
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert parquet to CSV with header - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleCSVSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/people.parquet"
    SimpleParquetConvertor.main(Array(
      "--source-format", "parquet",
      "--target-format", "csv",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.option("header", true).option("comment", "#").option("inferSchema", true).csv(testOutputDir)

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.parquet(srcPath)
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert parquet to JSON - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleJSONSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/people.parquet"
    SimpleParquetConvertor.main(Array(
      "--source-format", "parquet",
      "--target-format", "json",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.json(testOutputDir).withColumn("age", $"age".cast(IntegerType))

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.parquet(srcPath)
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert JSON to parquet - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleJSONParquetSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/people.json"
    SimpleParquetConvertor.main(Array(
      "--source-format", "json",
      "--target-format", "parquet",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.parquet(testOutputDir).withColumn("age", $"age".cast(IntegerType))

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.json(srcPath).withColumn("age", $"age".cast(IntegerType))
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert parquet to ORC - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleORCSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/people.parquet"
    SimpleParquetConvertor.main(Array(
      "--source-format", "parquet",
      "--target-format", "orc",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.orc(testOutputDir)

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.parquet(srcPath)
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert ORC to parquet - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleORCParquetSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/people.orc"
    SimpleParquetConvertor.main(Array(
      "--source-format", "orc",
      "--target-format", "parquet",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.parquet(testOutputDir)

    df.cache()
    assertResult(peopleDataCount)(df.count())
    assertResult(peopleDataSchemaTyped.fields.sortBy(_.name))(df.schema.fields.sortBy(_.name))

    val orig = spark.read.orc(srcPath)
      .orderBy($"first", $"last").as[Person].collectAsList()
    val converted = df.orderBy($"first", $"last").as[Person].collectAsList()

    assertResult(orig)(converted)
  }

  test("Convert text to parquet - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleTextParquetSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/lorem.txt"
    SimpleParquetConvertor.main(Array(
      "--source-format", "text",
      "--target-format", "parquet",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.parquet(testOutputDir)

    df.cache()
    assertResult(loremDataCount)(df.count())

    val orig = spark.read.text(srcPath).as[String].collect().sorted
    val converted = df.as[String].collect().sorted

    assertResult(orig)(converted)
  }

  test("Convert parquet to text - simple") {
    val testOutputDir = s"file:///tmp/testOutputPeopleTextSimple_${System.currentTimeMillis()}"
    val srcPath = "src/test/resources/data/lorem.parquet"
    SimpleParquetConvertor.main(Array(
      "--source-format", "parquet",
      "--target-format", "text",
      "--source-path", srcPath,
      "--target-path", testOutputDir))

    val df = spark.read.text(testOutputDir)

    df.cache()
    assertResult(loremDataCount)(df.count())

    val orig = spark.read.parquet(srcPath).as[String].collect().sorted
    val converted = df.as[String].collect().sorted

    assertResult(orig)(converted)
  }
}
