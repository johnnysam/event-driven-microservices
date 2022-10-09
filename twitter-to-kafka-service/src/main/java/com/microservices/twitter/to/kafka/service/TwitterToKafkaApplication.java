package com.microservices.twitter.to.kafka.service;

import com.microservices.twitter.to.kafka.service.config.TwitterToKafkaConfig;
import com.microservices.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j()
@RequiredArgsConstructor
@SpringBootApplication
public class TwitterToKafkaApplication implements CommandLineRunner {

    private final TwitterToKafkaConfig config;
    private final StreamRunner streamRunner;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Twitter-to-kafka is starting...");
        log.info(config.getTwitterKeywords().toString());
        log.info(config.getWelcomeMessage());
        streamRunner.start();
    }
}
