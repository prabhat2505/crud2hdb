package com.h2db.crudh2db.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.h2db.crudh2db.entity.SpaceCollector;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.persistence.Entity;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


@Component
public class KafkaUtility {
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String consumerBootstrapServer;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtility.class);

    public AdminClient getAdminClient() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, consumerBootstrapServer);
        return AdminClient.create(properties);
    }

    public Set<String> getAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsResult topics = adminClient.listTopics();
        return topics.names().get();
    }


    public TopicDescription checkTopicExist(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException, ResourceNotFoundException {

        ListTopicsResult topics = adminClient.listTopics();
        topics.names().get().forEach(System.out::println);
        // Get the List of Topics in server
        Set<String> topicList = adminClient.listTopics().names().get();
        // Get the details of all topics
        DescribeTopicsResult topicsResult = adminClient.describeTopics(topicList);
        System.out.println(topicsResult);
        // Check the topic exist by name
        final TopicDescription topicDescription = topicsResult.allTopicNames().get().get(topicName);
        // If not found throw exception
        if (topicDescription == null) {
            throw new ResourceNotFoundException("Topic - " + topicName + " not found !;");
        }
        System.out.println("Description of demo topic:" + topicDescription);
        return topicDescription;
    }

//    public Boolean validateJson(String message,Class<?> tClass) {
//        try {
//            ObjectMapper mapper = new ObjectMapper();
//            mapper.readValue(message,tClass);
//
//        }catch (JsonProcessingException jsonProcessingException){
//        LOGGER.error("Invalid Json message format:{}",jsonProcessingException.getMessage());
//        return false;
//        }
//        return true;
//    }

    public Boolean validateJson(String message,Class<SpaceCollector> spaceCollectorClass) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            SpaceCollector spaceCollector = mapper.readValue(message,spaceCollectorClass);
            System.out.println(spaceCollector.toString());
            if(spaceCollector.getId() == 0) {

            }
        }catch (JsonProcessingException jsonProcessingException){
            LOGGER.error("Invalid Json message format:{}",jsonProcessingException.getMessage());
            return false;
        }

        return true;
    }


}
