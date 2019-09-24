package com.example

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.collection.mutable

object BostonCrimesMap {
  // Median function
  def median(inputList: List[Long]): Long = {
    val sortedList = inputList.sorted
    val count: Int = inputList.size
    if (count % 2 == 0) {
      val l: Int = count / 2 - 1
      val r: Int = l + 1
      (sortedList(l) + sortedList(r)) / 2
    } else
      sortedList(count / 2)
  }

  // UDF for median function
  def medianUDF: UserDefinedFunction = udf((l: mutable.WrappedArray[Long]) => median(l.toList))

  // UDF for mkString
  def mkStringUDF: UserDefinedFunction = udf((l: mutable.WrappedArray[String]) => l.toList.mkString(", "))

  def main(args: Array[String]): Unit = {
    // Check number of arguments
    if (args.length != 3) {
      println("Incorrect arguments.")
      println("Usage: /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}")
      sys.exit(-1)
    }

    // Setup values from arguments
    val crimeFilename: String = args(0)
    val offenseCodesFilename: String = args(1)
    val resultFolder: String = args(2)

    // Create Spark Session
    val spark = SparkSession
      .builder
      .appName(" BostonCrimesMap")
      .getOrCreate()

    import spark.implicits._

    // Read Crime dataset, filter out Null District
    val crimeDF: DataFrame = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(crimeFilename)
      .filter($"DISTRICT".isNotNull)

    // Read Offense Codes dataset and broadcast it
    val offenseDF: DataFrame = spark
      .read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(offenseCodesFilename)
      .withColumn("crime_type", trim(split($"NAME", "-")(0)))

    val offenseBC: Broadcast[DataFrame] = spark.sparkContext.broadcast(offenseDF)

    // Join Crime with Offense Codes
    val crimeOffence: DataFrame = crimeDF
      .join(offenseBC.value, crimeDF("OFFENSE_CODE") === offenseBC.value("CODE"))
      // leave only import columns
      .select("INCIDENT_NUMBER", "DISTRICT", "MONTH", "Lat", "Long", "crime_type")
      // replace Null with 0 for Lat and Long
      .na.fill(0.0)
      .cache

    // Calculate crimes_total and crimes_monthly
    // Group by district and month and count crime incidents
    val result1 = crimeOffence
      .groupBy($"DISTRICT", $"MONTH")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
      // Group by district and calculate median
      .groupBy($"DISTRICT")
      .agg(sum($"crimes").alias("crimes_total"),
        collect_list($"crimes").alias("month_list"))
      .withColumn("crimes_monthly", medianUDF($"month_list"))
      .drop($"month_list")

    // Calculate frequent_crime_types
    val window: WindowSpec = Window.partitionBy($"DISTRICT").orderBy($"crimes".desc)

    // Group by district and crime_type
    val result2 = crimeOffence
      .groupBy($"DISTRICT", $"crime_type")
      .agg(count($"INCIDENT_NUMBER").alias("crimes"))
      .withColumn("rn", row_number().over(window))
      .filter($"rn" < 4)
      .drop($"rn")
      // Group by district, combine crime_types
      .groupBy($"DISTRICT")
      .agg(collect_list($"crime_type").alias("crime_type_list"))
      .withColumn("frequent_crime_types", mkStringUDF($"crime_type_list"))
      .drop($"crime_type_list")

    // Calculate average Lat and Long
    val result3 = crimeOffence
      .groupBy($"DISTRICT")
      .agg(mean($"Lat").alias("lat"),
        mean($"Long").alias("lng")
      )

    // Combine all together
    val result: DataFrame = result1
      .join(result2, Seq("DISTRICT"))
      .join(result3, Seq("DISTRICT"))

    result.repartition(1).write.mode("OVERWRITE").parquet(resultFolder)

    spark.stop()
  }
}