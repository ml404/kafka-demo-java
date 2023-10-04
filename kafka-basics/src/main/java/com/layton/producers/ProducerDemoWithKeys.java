package com.layton.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello Ruth");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create a ProducerRecord
        for (int i = 0; i < 2; i++) {

            for (int j = 0; j < 10; j++) {
                String topic = "demo_java";
                String key = "id_" + j;
                String value = "Hello Ruth " + j;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //Send data
                producer.send(producerRecord, (metadata, e) -> {
                    //executes everytime a record is sent or an exception is thrown.
                    if (e == null) {
                        //record was successfully sent.
                        log.info("Key: " + key + "| Partition: " + metadata.partition());
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
