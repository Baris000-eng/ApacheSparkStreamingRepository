package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Hello world!
 *
 */
public class Application
{
    public static void main( String[] args ) throws StreamingQueryException {

        System.setProperty("hadoop.home.dir","/Users/barissss/Desktop/hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("streamingTimeOperations").master("local").getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream().format("socket").option("host","localhost").option("port","8000").option("includeTimestamp",true).load();


        //Samsung Galaxy S22 -> 10.09.2022 22:29

        Dataset<Row> products = rawData.as(Encoders.tuple(Encoders.STRING(),Encoders.TIMESTAMP())).toDF("product","timestamp");

        Dataset<Row> resultData = products.groupBy(functions.window(products.col("timestamp"),"1 minute"),products.col("product")).count().orderBy("window");
        StreamingQuery startQuery = resultData.writeStream().outputMode("complete").format("console").option("truncate","false").start();

        startQuery.awaitTermination();










    }
}
