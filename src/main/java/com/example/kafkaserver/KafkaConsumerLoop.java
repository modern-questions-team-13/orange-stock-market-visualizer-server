package com.example.kafkaserver;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerLoop implements Runnable {
    private final KafkaConsumer<String, KafkaData.KafkaEntry> kafkaConsumer;
    private final List<String> topics;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;

    private final KafkaData kafkaData;


    public KafkaConsumerLoop(List<String> topics, KafkaData kafkaData) throws UnknownHostException {
        this.kafkaData = kafkaData;
        Properties config = new Properties();
        config.put("client.id", InetAddress.getLocalHost().getHostName());
        config.put("group.id", "MyKafkaSpring");
        config.put("bootstrap.servers", "localhost:9091");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer","org.springframework.kafka.support.serializer.JsonDeserializer");
        config.put("spring.json.value.default.type", KafkaData.KafkaEntry.class);
        this.kafkaConsumer = new KafkaConsumer<>(config);

        List<TopicPartition> tp = new ArrayList<>();
        topics.forEach(topic -> tp.add(new TopicPartition(topic, 0)));

/// расскоменти эти строчки, чтобы сбрасывать оффсет (ведет к странному поведению)
//        kafkaConsumer.assign(tp);
//
//        kafkaConsumer.poll(Duration.ofSeconds(1));
//        kafkaConsumer.seekToBeginning(tp);
//
//        kafkaConsumer.commitSync();
//        kafkaConsumer.unsubscribe();


        this.topics = topics;
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }


    @Override
    public void run() {
        try {
            kafkaConsumer.subscribe(topics);

            while (!shutdown.get()) {
                ConsumerRecords<String, KafkaData.KafkaEntry> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> kafkaData.putData(record.value()));
            }
        } finally {
            kafkaConsumer.close();
            shutdownLatch.countDown();
        }
    }
    public void shutdown() throws InterruptedException {
        shutdown.set(true);
        shutdownLatch.await();
    }
}
