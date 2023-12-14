package com.h2db.crudh2db.controller;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.h2db.crudh2db.entity.SpaceCollector;
import com.h2db.crudh2db.kafka.KafkaProducer;
import com.h2db.crudh2db.utils.KafkaUtility;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.websocket.server.PathParam;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/v1/kafka")
public class MessageController {

    private KafkaProducer kafkaProducer;
    private KafkaUtility kafkaUtility;

    public MessageController(KafkaProducer kafkaProducer,KafkaUtility kafkaUtility) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaUtility = kafkaUtility;
    }

    // http:localhost:8080/api/v1/kafka/publish?message=hello world
    @GetMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam("message") String message){
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok("Message sent to the topic");
    }
    @GetMapping("/check/{topic}")
    public ResponseEntity<TopicDescription> check(@PathVariable("topic") String topic) throws ExecutionException, InterruptedException {
        AdminClient adminClient = kafkaUtility.getAdminClient();
        TopicDescription topicDescription = kafkaUtility.checkTopicExist(adminClient,topic);
        return new ResponseEntity<>(topicDescription, HttpStatus.CREATED);
    }

    @GetMapping("/validate")
    public ResponseEntity<Boolean> validate() throws JsonProcessingException, UnrecognizedPropertyException {
        String message="{\"url\":\"sonoo\"}";
//        String message="hhhh";
        Boolean isJson = kafkaUtility.validateJson(message, SpaceCollector.class);
        return new ResponseEntity<>(isJson, HttpStatus.CREATED);
    }
}
