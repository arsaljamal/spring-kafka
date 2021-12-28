package com.arsal.kafkaproducer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaRestController {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/create/topic")
    public void createTopicWithAdmin(@RequestParam(value = "topic") String topic) {
        short replicationFactor=1;
        int numberOfPartition=1;
        kafkaAdmin.createOrModifyTopics(new NewTopic(topic, numberOfPartition, replicationFactor));
    }

    @PostMapping("/create/message")
    public void createMessageWithTemplate(@RequestParam(value = "topic") String topic, @RequestParam(value = "message") String message) {
        sendMessage(topic, message);
    }

    public void sendMessage(String topic, String message) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Failure with following error :" + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Message : " + message + ", Offset : " + result.getRecordMetadata().offset());
            }
        });
    }
}
