package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;

public class IotWeatherApplication {
    public static void main(String[] args) throws StreamingQueryException {

        //command I used to connect to the port 8000 in netcat: nc -l localhost -p 8000

        System.setProperty("hadoop.home.dir","/Users/barissss/Desktop/hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().appName("IotWeatherApplication").master("local").getOrCreate();
        StructType weather = new StructType().add("district","string").add("heatType","string").add("heat","integer").add("windType","string").add("wind","integer");

        Dataset<Row> data = sparkSession.readStream().schema(weather).option("sep", ",").csv("/Users/barissss/IdeaProjects/SparkStreamingListenerApplication/*");
        System.out.println(data);

        Dataset<Row> heatData = data.select("district","heat").where("heat>25");

        System.out.println(heatData);

        StreamingQuery startQuery = heatData.writeStream().format("console").start();

        startQuery.awaitTermination();













    }
}
