package com.example.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example"})
@Slf4j
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }
} 