package com.dde

import java.util.Properties
import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.piggybank.storage._
import org.apache.pig.backend.hadoop.executionengine.tez._

object Main
{
	def main(args: Array[String]): Unit = {
		val start = System.currentTimeMillis()
		val prop = new Properties()
		prop.setProperty("pig.exec.mapPartAgg", "true")
		prop.setProperty("pig.exec.mapPartAgg.minReduction", "10")
		prop.setProperty("mapred.tasktracker.map.tasks.maximum", "1000")
		prop.setProperty("mapred.tasktracker.reduce.tasks.maximum", "1000")
		prop.setProperty("pig.schematuple", "on")
		prop.setProperty("pig.tmpfilecompression", "true")
		prop.setProperty("pig.tmpfilecompression.codec", "lzo")

		val pigServer = new PigServer("local", prop)
		pigServer.debugOff()
		pigServer.registerQuery("temp = LOAD '/Users/daviddecoding/Misc/BigD/GlobalLandTemperaturesByCity.csv' USING org.apache.pig.piggybank.storage.CSVLoader() as (dt:chararray,avg_temp:double,avg_temp_uncert:chararray,city:chararray,country:chararray,lat:chararray,longi:chararray);")
        pigServer.registerQuery("temp = GROUP temp BY (city,country) PARALLEL 5;")
		pigServer.registerQuery("temp = FOREACH temp GENERATE group, MAX(temp.avg_temp);")
		pigServer.store("temp", "/Users/daviddecoding/Misc/BigD/pig-output", "org.apache.pig.piggybank.storage.CSVExcelStorage()")
		val end = System.currentTimeMillis()
		println("The total time taken: " + ((end - start) / 1000))
	}
}
