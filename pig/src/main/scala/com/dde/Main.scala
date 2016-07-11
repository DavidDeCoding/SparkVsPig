package com.dde

import java.util.Properties
import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.piggybank.storage._
import org.apache.pig.backend.hadoop.executionengine.tez._

object Main
{
	def main(args: Array[String]): Unit = {
		val prop = new Properties()
		prop.setProperty("pig.splitCombination", "false")
		prop.setProperty("pig.exec.mapPartAgg", "true")
		val pigServer = new PigServer("tez_local", prop)

    		pigServer.debugOn()
                pigServer.setDefaultParallel(5)
		pigServer.registerQuery("temp = LOAD '/home/davidde/GlobalLandTemperaturesByCity.csv' USING org.apache.pig.piggybank.storage.CSVLoader() as (dt:chararray,avg_temp:double,avg_temp_uncert:chararray,city:chararray,country:chararray,lat:chararray,longi:chararray);")
		pigServer.registerQuery("temp = FOREACH temp GENERATE city, country, avg_temp;")
                pigServer.registerQuery("temp = GROUP temp BY (city,country);")
		pigServer.registerQuery("temp = FOREACH temp GENERATE FLATTEN(group), MAX(temp.avg_temp);")
		pigServer.store("temp", "/home/davidde/pig-output", "org.apache.pig.piggybank.storage.CSVExcelStorage()")
	}
}
