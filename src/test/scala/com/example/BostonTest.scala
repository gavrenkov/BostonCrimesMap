package com.example

import org.scalatest.FunSuite
import com.example.BostonCrimesMap._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark test")
      .getOrCreate()
  }

}
class BostonTest extends FunSuite with SparkSessionTestWrapper {
  import spark.implicits._

  test("Median test unsorted") {
    val data: List[Long] = List(11L, 9L, 3L, 5L, 5L)
    assert(median(data) === 5L)
  }

  test("Median test sorted") {
    val data: List[Long] = List(3L, 5L, 5L, 9L, 11L)
    assert(median(data) === 5L)
  }

  test("Median UDF") {
    val testDF: DataFrame = Seq(
      ("A1", Array(11L, 9L, 3L, 5L, 5L)),
      ("B2", Array(3L, 5L, 5L, 9L, 11L))
    ).toDF("district", "month_list")

    val resultDF: DataFrame = testDF
      .withColumn("crimes_monthly", medianUDF($"month_list"))
      .drop($"month_list")

    val expectedSchema = List(
      StructField("district", StringType, true),
      StructField("crimes_monthly", LongType, false)
    )

    val expectedData = Seq(
      Row("A1", 5L),
      Row("B2", 5L)
    )

    val expectedDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(resultDF.schema == expectedDF.schema && resultDF.except(expectedDF).count == 0)
  }

  test("mkStringUDF") {
    val testDF: DataFrame = Seq(
      ("A1", Array("one", "two", "three")),
      ("B2", Array("four", "five"))
    ).toDF("district", "crime_type_list")

    val resultDF: DataFrame = testDF
      .withColumn("frequent_crime_types", mkStringUDF($"crime_type_list"))
      .drop($"crime_type_list")

    val expectedSchema = List(
      StructField("district", StringType, true),
      StructField("frequent_crime_types", StringType, true)
    )

    val expectedData = Seq(
      Row("A1", "one, two, three"),
      Row("B2", "four, five")
    )

    val expectedDF: DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(resultDF.schema == expectedDF.schema && resultDF.except(expectedDF).count == 0)
  }
}
