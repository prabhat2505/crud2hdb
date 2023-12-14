package com.h2db.crudh2db.utils;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import static org.junit.jupiter.api.Assertions.*;


@TestPropertySource("/application-test.yml")
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
public class KafkaUtilityTests {
    @Autowired
    public KafkaTemplate<String, String> template;
    @Autowired
    private  KafkaUtility kafkaUtility;
    private String TOPIC_NAME = "test-topic";
    private String INVALID_TOPIC = "invalid-topic";
    private Integer NUM_PARTITIONS = 1;
    private short REPLICATION_FACTOR = 1;
    private static final Collection<String> TOPIC_LIST = Arrays.asList("CustomerCountry", "Products", "Sample");

    @Test
    void givenTopicNotPresent_whenChecked_thenThrowException() throws ExecutionException, InterruptedException {
        AdminClient adminClient = kafkaUtility.getAdminClient();
        assertThrows(ResourceNotFoundException.class,
                () -> kafkaUtility.checkTopicExist(adminClient,INVALID_TOPIC));
        adminClient.close();
    }
}
