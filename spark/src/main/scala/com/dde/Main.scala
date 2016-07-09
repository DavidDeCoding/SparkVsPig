package com.dde

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Main
{
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[4]").setAppName("Playing with Fire")
		val sc = new SparkContext(conf)
		val sql = new SQLContext(sc)
		val df = sql.read
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.option("inferSchema", "true")
			.load("/home/davidde/GlobalLandTemperaturesByCity.csv")

		val dfByCity = df.groupBy("City", "Country").agg(max("AverageTemperature"))
		dfByCity.write
			.format("com.databricks.spark.csv")
			.option("header", "true")
			.save("/home/davidde/spark-output")
	}
}
