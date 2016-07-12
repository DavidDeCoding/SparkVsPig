package com.dde

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};
import com.opencsv._
import org.apache.spark.sql._

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
				StructField("City", StringType, false),
				StructField("Country", StringType, false),
				StructField("Latitude", StringType, true),
				StructField("Longitutde", StringType, true)))

		class MyCSVParser(separator: Char) extends CSVParser(separator) with Serializable
		val csvParser = new MyCSVParser(',')

		val df = sql.read
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("nullValue", "")
			.option("mode", "FAILFAST")
			.option("parserLib", "univocity")
			.schema(schema)
			.load("file:///Users/davidde/Personal/BigD/GlobalLandTemperaturesByCity.csv")
		// val csvRdd = sc.textFile("/Users/davidde/Personal/BigD/GlobalLandTemperaturesByCity.csv")
		// 				.filter(line => !line.contains("AverageTemperature"))
		// 				.map(line => csvParser.parseLine(line))
		// 				.filter(cells => !cells(1).isEmpty)
		// 				.map(cells => Row(cells(0), cells(1).toDouble, cells(2), cells(3), cells(4), cells(5), cells(6)))
		// val df = sql.createDataFrame (csvRdd, schema)

		import sql.implicits._
		val dfByCity = df.repartition(8, $"City", $"Country")
		dfByCity.explain()
		dfByCity.select("City", "Country", "AverageTemperature").groupBy("City", "Country").agg(max("AverageTemperature")).write
				.format("json")
				// .format("com.databricks.spark.csv")
				// .option("header", "false")
				.save("/Users/davidde/Personal/BigD/spark-output")
		val end = System.currentTimeMillis()
		println("Total Time Taken By the process: " + ((end - start) / 1000))
	}
}
