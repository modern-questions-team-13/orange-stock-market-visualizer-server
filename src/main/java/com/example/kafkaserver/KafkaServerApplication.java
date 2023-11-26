package com.example.kafkaserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class KafkaServerApplication {

    public static void main(String[] args) throws UnknownHostException {
        List<String> topics = new ArrayList<>();
        topics.add("TestTopic");

        KafkaData kafkaData = new KafkaData();
        Thread t = new Thread(new KafkaConsumerLoop(topics, kafkaData));
        t.start();
//
//        KafkaConsumerLoop l = new KafkaConsumerLoop(topics);
//        l.run();
        SpringApplication.run(KafkaServerApplication.class, args);
    }

}
