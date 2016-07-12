package com.manny;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import java.util.regex.Pattern;

public class WeatherTestRefl
{
    public static void main (String[] args) throws Exception
    {
        Runtime.getRuntime().exec("rm -rf /Users/mbacolas/Downloads/GlobalLandTemperatures/spark-output").waitFor ();

        long start = System.currentTimeMillis ();
        SparkConf sparkConf = new SparkConf ();
        sparkConf.setMaster ("local[8]");
        sparkConf.setAppName ("JavaSparkSQL");
        sparkConf.setJars (new String[] { "/Users/mbacolas/hrv_projects/hrv_20_percent_time/build/libs/spark_poc-1.0-SNAPSHOT.jar" });
        sparkConf.set ("spark.cores.max", "8");
        sparkConf.set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set ("spark.executor.memory", "5g");

        JavaSparkContext ctx = new JavaSparkContext (sparkConf);
        SQLContext sqlContext = new SQLContext (ctx);

        Function<String, WeatherMeasurement> toWM = line -> {
            //dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
            String[] parts = line.split (Pattern.quote (","));

            WeatherMeasurement weatherMeasurement = new WeatherMeasurement();
            weatherMeasurement.setDt (parts[ 0 ]);
            if (parts[ 1 ] != null && parts[ 1 ].trim ().length () > 0 && ! parts[ 1 ].equalsIgnoreCase ("AverageTemperature")) weatherMeasurement.setAverageTemperature ( Double.valueOf (parts[ 1 ]) );
            weatherMeasurement.setAverageTemperatureUncertainty (parts[ 2 ]);
            weatherMeasurement.setCity (parts[ 3 ]);
            weatherMeasurement.setCountry (parts[ 4 ]);
            weatherMeasurement.setLatitude (parts[ 5 ]);
            weatherMeasurement.setLongitude (parts[ 6 ]);

            return new WeatherMeasurement();
        };

        JavaRDD<WeatherMeasurement> rdd = ctx.textFile ("/Users/mbacolas/Downloads/GlobalLandTemperatures/GlobalLandTemperaturesByCity.csv").map (toWM);
        DataFrame dataFrame = sqlContext.createDataFrame(rdd, WeatherMeasurement.class).repartition (8, new Column ("country"), new Column ("city"));
        dataFrame.explain ();
        DataFrame maxTemp = dataFrame.groupBy ("city", "country").max ("averageTemperature");

        maxTemp.write ().format ("json").save ("/Users/mbacolas/Downloads/GlobalLandTemperatures/spark-output");

        long end = System.currentTimeMillis ();
        long total = end - start;
        System.out.println ("\n\n*********************************** total: " + total);
    }
}
