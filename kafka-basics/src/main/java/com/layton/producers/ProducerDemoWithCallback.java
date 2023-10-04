package com.layton.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello Ruth");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create a ProducerRecord

        for (int i = 0; i < 10; i++) {

            for (int j = 0; j < 30; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello ruth " + i + " " + j);
                //Send data
                producer.send(producerRecord, (metadata, e) -> {
                    //executes everytime a record is sent or an exception is thrown.
                    if (e == null) {
                        //record was successfully sent.
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error whilst producing", e);
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //Flush and close producer
        producer.flush();
        producer.close();
    }
}
