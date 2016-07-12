package com.dde

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};

object Main
{
	def main(args: Array[String]): Unit = {
		val start = System.currentTimeMillis()
		val conf = new SparkConf().setMaster("local[8]").setAppName("Playing with Fire")
		val sc = new SparkContext(conf)
		val sql = new SQLContext(sc)
		val schema = StructType(Array(
				StructField("date", StringType, true),
				StructField("AverageTemperature", DoubleType, true),
				StructField("AverageTemperatureUncertainty", StringType, true),
				StructField("City", StringType, true),
				StructField("Country", StringType, true),
				StructField("Latitude", StringType, true),
				StructField("Longitutde", StringType, true)))
		val df = sql.read
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.schema(schema)
			.load("/Users/daviddecoding/Misc/BigD/GlobalLandTemperaturesByCity.csv")

		import sql.implicits._ 
		val dfByCity = df.repartition($"City", $"Country").select("City", "Country", "AverageTemperature").groupBy("City", "Country").agg(max("AverageTemperature"))
		dfByCity.write
			.format("com.databricks.spark.csv")
			.option("header", "false")
			.save("/Users/daviddecoding/Misc/BigD/spark-output")
		val end = System.currentTimeMillis()
		println("Total Time Taken By the process: " + ((end - start) / 1000))
	}
}
