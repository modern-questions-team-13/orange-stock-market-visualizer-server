package com.example.kafkaserver.controllers;

import com.example.kafkaserver.KafkaData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@CrossOrigin
@RestController
public class KafkaController {

    record Company(Integer id, String name) {}
    private final List<Company> companies = new ArrayList<>();
    private final KafkaData kafkaData;

    public KafkaController(@Autowired KafkaData kafkaData) {
        this.kafkaData = kafkaData;
        companies.add(new Company(12, "Test"));
        companies.add(new Company(10, "Test1"));
    }

//    @CrossOrigin
    @GetMapping("/companies/{id}/{type}/{count}")
    List<KafkaData.KafkaEntry> getComps(@PathVariable Integer id, @PathVariable Integer type, @PathVariable Integer count) {
//        System.out.println(kafkaData.data.get(id));

        if (kafkaData.data.get(id) == null) {
            return new ArrayList<>();
        }
        List<KafkaData.KafkaEntry> filtered = kafkaData.data.get(id).stream().filter(entry -> entry.type().equals(type)).collect(Collectors.toCollection(ArrayList::new));
        if (filtered.size() < count) {
            return filtered;
        }

        return filtered.subList(filtered.size() - count, filtered.size());
    }


    @GetMapping("/companies")
    List<Company> getCompanies() {
        return companies;
    }
}
