package com.layton.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello Ruth");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create a ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello ruth");
        //Send data
        producer.send(producerRecord);
        //Flush and close producer
        producer.flush();
        producer.close();
    }
}
