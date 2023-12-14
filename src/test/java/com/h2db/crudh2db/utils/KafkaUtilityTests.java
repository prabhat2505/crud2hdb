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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;

@TestPropertySource("/application-test.yml")
@SpringBootTest
@DirtiesContext
//@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
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
    public void givenTopic_whenChecked() throws ExecutionException, InterruptedException {
           AdminClient adminClient = kafkaUtility.getAdminClient();

//        Map<String, String> newTopicConfig = new HashMap<>();
//        newTopicConfig.put(TopicConfig.CLEANUP_POLICY_COMPACT, TopicConfig.CLEANUP_POLICY_COMPACT);

        CreateTopicsResult newTopic = adminClient.createTopics
                (Collections.singletonList(new NewTopic(TOPIC_NAME,
                        NUM_PARTITIONS, REPLICATION_FACTOR)));
        //newTopic.config()
        //adminClient.describeTopics();
        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);

        DescribeTopicsResult
                demoTopic=adminClient.describeTopics(topics.names().get());
        System.out.println(demoTopic);
        final TopicDescription topicDescription = demoTopic.values().get(TOPIC_NAME).get();
        System.out.println("Description of demo topic:" + topicDescription);
//
//        TopicDescription topicDescription =
//                demoTopic.values().get(TOPIC_NAME).get();



    }
    @Test
    void givenTopicNotPresent_whenChecked_thenThrowException() throws ExecutionException, InterruptedException {
        //given No topic
        AdminClient adminClient = kafkaUtility.getAdminClient();
//        Set<String> topicListDeletion = kafkaUtility.getAllTopics(adminClient);
//        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicListDeletion);
//        deleteTopicsResult.all().get();

//        CreateTopicsResult newTopic = adminClient.createTopics
//                (Collections.singletonList(new NewTopic(TOPIC_NAME,
//                        NUM_PARTITIONS, REPLICATION_FACTOR)));
//
//        Set<String> topics = Set.of(TOPIC_NAME);
//        adminClient.deleteTopics();
//        assertEquals(topics,kafkaUtility.getAllTopics(adminClient));


        //given(kafkaUtility.getAllTopics(adminClient)).willReturn(topics);
        //System.out.println(kafkaUtility.checkTopicExist(adminClient,TOPIC_NAME));
        //System.out.println(kafkaUtility.checkTopicExist(adminClient,INVALID_TOPIC));
        assertThrows(ResourceNotFoundException.class,
                () -> kafkaUtility.checkTopicExist(adminClient,INVALID_TOPIC));
//        kafkaUtility.checkTopicExist(adminClient,"kkk");
        //when passing wrong
        //kafkaUtility.checkTopicExist("test2-topic");

    }

//    @Test
//    void givenTopicName_whenCreateNewTopic_thenTopicIsCreated() throws Exception {
//        kafkaTopicApplication.createTopic("test-topic");
//
//        String topicCommand = "/usr/bin/kafka-topics --bootstrap-server=localhost:9092 --list";
//        String stdout = KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", topicCommand)
//                .getStdout();
//
//        assertThat(stdout).contains("test-topic");
//    }
}
