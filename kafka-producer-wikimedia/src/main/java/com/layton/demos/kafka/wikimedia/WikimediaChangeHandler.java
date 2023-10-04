package com.layton.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> producer, String topic) {

        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        //NO-OP
    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {
        //NO-OP
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading", t);
    }
}
