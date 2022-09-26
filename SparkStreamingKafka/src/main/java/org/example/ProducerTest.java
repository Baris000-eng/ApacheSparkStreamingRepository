package org.example;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;

public class ProducerTest {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String topic = "search";
        Gson gson = new Gson();
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Producer producer = new KafkaProducer<String,String>(properties);

        while(true) {
            System.out.println("Search: ");
            String productData = scanner.nextLine();
            SearchProductModel searchProductModel = new SearchProductModel();
            searchProductModel.setProductName(productData);
            String timeData = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            searchProductModel.setTime(timeData);
            String json = gson.toJson(searchProductModel);
            System.out.println("Json Format: ");
            System.out.println(json);

            //first string = topic name, second string = data
            ProducerRecord<String,String>record = new ProducerRecord<String,String>(topic,json);
            producer.send(record);
            System.out.println("Data is sent to kafka");

        }


    }
}
