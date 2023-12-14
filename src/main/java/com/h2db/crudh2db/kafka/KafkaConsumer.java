package com.h2db.crudh2db.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class KafkaConsumer {
    @Value("${spring.kafka.topic.name}")
    private  String kafkaTopic;
    public  String receivedMessage;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);
//    public String getPayload() {
//        return receivedMessage;
//    }
//    @KafkaListener(topics = "${spring.kafka.topic.name}")
//    public void consume(String message){
//        LOGGER.info(String.format("Message received -> %s", message));
//        LOGGER.info(String.format("kafkaTopic -> %s", kafkaTopic));
//        this.receivedMessage = message;
//    }




    private CountDownLatch latch = new CountDownLatch(1);
    private String payload;

    @KafkaListener(topics = "${spring.kafka.topic.name}")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        LOGGER.info(String.format("kafkaTopic -> %s", kafkaTopic));
        LOGGER.info("received payload='{}'", consumerRecord.toString());
        payload = consumerRecord.toString();
        latch.countDown();
    }

    public void resetLatch() {
        latch = new CountDownLatch(1);
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
