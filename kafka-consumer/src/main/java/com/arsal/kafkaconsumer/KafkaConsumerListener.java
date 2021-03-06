package com.arsal.kafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerListener {

    @KafkaListener(topics = "sampletopic", groupId = "group-id")
    public void genericMessageListener(String message) {
        System.out.println("Message :" + message + " Received by Consumer.");
    }

    @KafkaListener(
            topics = "student",
            containerFactory = "studentKafkaListenerContainerFactory")
    public void studentListener(Student student) {
        System.out.println("Student :" + student.toString() + " Received by Consumer.");
    }
}
