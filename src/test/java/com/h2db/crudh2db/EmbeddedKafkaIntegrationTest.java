package com.h2db.crudh2db;

import com.h2db.crudh2db.kafka.KafkaConsumer;
import com.h2db.crudh2db.kafka.KafkaProducer;
import net.mguenther.kafka.junit.ExternalKafkaCluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;
@SpringBootTest
@DirtiesContext

@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
public class EmbeddedKafkaIntegrationTest {
    @Autowired
    public KafkaTemplate<String, String> template;
    @Autowired
    private KafkaConsumer consumer;

    @Autowired
    private KafkaProducer producer;
    @Container
    private KafkaContainer kafkaContainer = new KafkaContainer();

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived()
            throws Exception {
        String data = "Sending with our own simple KafkaProducer";

        producer.sendMessage(data);

        boolean messageConsumed = consumer.getLatch().await(10, TimeUnit.SECONDS);
        assertTrue(messageConsumed);

    }
//    @Test
//    @DisplayName("should be able to observe records written to an external Kafka cluster")
//    void externalKafkaClusterShouldWorkWithExternalResources() throws Exception {
//        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//        Set<String> topicNames = topics.keySet();
//    }
}
