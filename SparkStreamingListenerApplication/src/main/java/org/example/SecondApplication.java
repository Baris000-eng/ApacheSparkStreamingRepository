package org.example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

//output mode = update
public class SecondApplication {
    public static void main(String[]args) throws StreamingQueryException, TimeoutException {
        System.setProperty("hadoop.home.dir","/Users/barissss/Desktop/hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("SparkStreamingMessageListenerApplication").master("local").getOrCreate();
        Dataset<Row> data = sparkSession.readStream().format("socket").option("host","localhost").option("port","8000").load();
        Dataset<String> myData = data.as(Encoders.STRING());
        Dataset<String> stringSet = myData.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String string) throws Exception {
                String[]array = string.split(" ");
                return Arrays.asList(array).iterator();
            }
        },Encoders.STRING());

        Dataset<Row> groupedData = stringSet.groupBy("value").count();
        StreamingQuery start = groupedData.writeStream().outputMode("update").format("console").start();
        start.awaitTermination();

    }


}
