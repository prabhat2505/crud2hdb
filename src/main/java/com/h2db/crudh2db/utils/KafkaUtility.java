package com.h2db.crudh2db.utils;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
//import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
//import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;

@Component
public class KafkaUtility {
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String consumerBootstrapServer;

    public  AdminClient getAdminClient() {
        System.out.println("admin"+consumerBootstrapServer);
        Properties properties = new Properties();


        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, consumerBootstrapServer);
        return AdminClient.create(properties);
    }

    public  Set<String> getAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsResult topics = adminClient.listTopics();
        return topics.names().get();
    }
    //Check Topic is present
//public  void checkTopicExist(AdminClient adminClient ,String topicName) throws ExecutionException, InterruptedException {
    public  TopicDescription checkTopicExist(AdminClient adminClient,String topicName) throws ExecutionException, InterruptedException,ResourceNotFoundException {

        Set<String> topicList = adminClient.listTopics().names().get();
//        DescribeTopicsResult topicsResult=adminClient.describeTopics(topicList);
//        //DescribeTopicsResult topicsResult=adminClient.describeTopics(adminClient.listTopics().names().get());
//        TopicDescription topicDescription = topicsResult.allTopicNames().get().get(topicName);
//        if(topicDescription == null) {
//            throw new ResourceNotFoundException("Topic - "+topicName+" not found !;");
//        }
//        //return topicsResult.allTopicNames().get().get(topicName);
//        //return topicsResult.values().get(topicName).get();

        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);

        DescribeTopicsResult topicsResult = adminClient.describeTopics(topicList);
        System.out.println(topicsResult);
        final TopicDescription topicDescription = topicsResult.values().get(topicName).get();
        System.out.println("Description of demo topic:" + topicDescription);
        return topicDescription;
    }

    //Check Topic is accessible
//    public  TopicDescription checkTopicAccessible(String topicName) throws ExecutionException, InterruptedException {
//        AdminClient adminClient = getAdminClient();
//        Set<String> topicList = getAllTopics();
//        DescribeTopicsResult demoTopic=adminClient.describeTopics(topicList);
//        //return demoTopic.allTopicNames().get().get(topicName);
//        return demoTopic.values().get(topicName).get();
//    }

    //validate json received from topic
//    public boolean validateMessage(String topic){
//        ObjectMapper mapper = new ObjectMapper();
//        try {
//            var grant = mapper.readValue(topic, Person.class);
//            System.out.println(grant);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        }
//
//    }

}
