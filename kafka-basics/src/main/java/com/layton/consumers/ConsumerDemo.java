package com.layton.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    private static String groupId = "my-java-application";
    private static String topic = "demo_java";

    public static void main(String[] args) {
        log.info("I am a consumer");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        //Set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));

        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                log.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                log.info(String.format("Partition: %s, Offset: %s", record.partition(), record.offset()));
            }
        }

    }
}
