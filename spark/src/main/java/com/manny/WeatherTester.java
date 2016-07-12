package com.manny;

import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;


public class WeatherTester
{
    public static void main (String[] args) throws Exception
    {
        long start = System.currentTimeMillis ();

        SparkConf sparkConf = new SparkConf ();
//        sparkConf.setMaster ("local[8]");
        sparkConf.setMaster ("local[8]");
        sparkConf.setAppName ("JavaSparkSQL");
        sparkConf.set ("spark.cores.max", "8");
        sparkConf.set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set ("spark.executor.memory", "5g");
//    sparkConf.validateSettings ();

        JavaSparkContext ctx = new JavaSparkContext (sparkConf);
        SQLContext sqlContext = new SQLContext (ctx);

        //dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
        List<StructField> fields = new ArrayList<StructField> ();
        fields.add (DataTypes.createStructField ("dt", DataTypes.StringType, true));
        fields.add (DataTypes.createStructField ("AverageTemperature", DataTypes.DoubleType, true));
        fields.add (DataTypes.createStructField ("AverageTemperatureUncertainty", DataTypes.StringType, true));
        fields.add (DataTypes.createStructField ("City", DataTypes.StringType, false));
        fields.add (DataTypes.createStructField ("Country", DataTypes.StringType, false));
        fields.add (DataTypes.createStructField ("Latitude", DataTypes.StringType, true));
        fields.add (DataTypes.createStructField ("Longitude", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType (fields);

        JavaRDD rdd = ctx.textFile ("/Users/davidde/Personal/BigD/GlobalLandTemperaturesByCity.csv");
        // DataFrame peopleDataFrame = sqlContext.read()
        //                                       .format("com.databricks.spark.csv")
        //                                       .option("header", "true")
        //                                       .option("parseLib", "univocity")
        //                                       .schema(schema)
        //                                       .load("/Users/davidde/Personal/BigD/GlobalLandTemperaturesByCity.csv")
        //                                       .repartition (8, new Column ("Country"), new Column ("City"));

        JavaRDD<Row> rowRDD = rdd.map (
                new Function<String, Row> ()
                {
                    public Row call (String line) throws Exception
                    {
                        String[] parts = line.split (Pattern.quote (","));

                        double avgTemp = 0;
                        if (parts[ 1 ] != null && parts[ 1 ].trim ().length () > 0 && ! parts[ 1 ].equalsIgnoreCase ("AverageTemperature"))
                            avgTemp = Double.valueOf (parts[ 1 ]);

                        return RowFactory.create (parts[ 0 ], avgTemp, parts[ 2 ], parts[ 3 ], parts[ 4 ], parts[ 5 ], parts[ 6 ]);
                    }
                });

        DataFrame peopleDataFrame = sqlContext.createDataFrame (rowRDD, schema).repartition (8, new Column ("Country"), new Column ("City"));
        peopleDataFrame.explain ();

        //DataFrame avgTemp = peopleDataFrame.groupBy("City", "Country").avg ("AverageTemperature");
        DataFrame maxTemp = peopleDataFrame.groupBy ("City", "Country").max ("AverageTemperature");
        //avgTemp.show (20);
        //maxTemp.show (20);

        maxTemp.write ().format ("json").save ("/Users/davidde/Personal/BigD/spark-output");
//    maxTemp.write ().format("com.databricks.spark.csv").save ("/Users/mbacolas/Downloads/GlobalLandTemperatures/spark-output");

        long end = System.currentTimeMillis ();
        long total = end - start;
        System.out.println ("total: " + (total / 1000));
    }
}
