package com.h2db.crudh2db;


import net.mguenther.kafka.junit.ExternalKafkaCluster;
import net.mguenther.kafka.junit.ReadKeyValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class ExternalKafkaClusterTest {
    @Test
    @DisplayName("should be able to observe records written to an external Kafka cluster")
    void externalKafkaClusterShouldWorkWithExternalResources() throws Exception {
        ExternalKafkaCluster kafka = ExternalKafkaCluster.at("localhost:9092");
        System.out.println(kafka.exists("test-topic"));
        Properties topicConfig = kafka.fetchTopicConfig("javaguides");
        System.out.println("tttttttttttttttttopic"+topicConfig);
    }

}
