package com.layton.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class);
    private static final String topic = "wikimedia.recentchange";
    private static final String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //Set producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //high throughput producer properties
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        WikimediaChangeHandler wikimediaChangeHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(wikimediaChangeHandler, builder).build();
        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }
}
