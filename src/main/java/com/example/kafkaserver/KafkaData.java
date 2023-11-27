package com.example.kafkaserver;

import org.apache.kafka.common.protocol.types.Field;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KafkaData {
    public record KafkaEntry (Integer type, Integer companyId, Float price, String datetime) {}

    public static volatile Map<Integer, List<KafkaEntry>> data = new HashMap<>();

    public void putData(KafkaEntry entry) {
        if (!data.containsKey(entry.companyId)) {
            data.put(entry.companyId, new ArrayList<>());
        }

        System.out.println(entry);
        if (data.get(entry.companyId).size() > 1000) {
            data.get(entry.companyId).remove(0);
        }
        data.get(entry.companyId).add(entry);
    }
}
