package com.microservices.twitter.to.kafka.service.runner;

import com.microservices.twitter.to.kafka.service.config.TwitterToKafkaConfig;
import com.microservices.twitter.to.kafka.service.helper.TwitterV2StreamHelper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaConfig config;

    private final TwitterV2StreamHelper helper;

    public TwitterV2KafkaStreamRunner(TwitterToKafkaConfig config, TwitterV2StreamHelper helper) {
        this.config = config;
        this.helper = helper;
    }

    @Override
    public void start() {
        val bearerToken = config.getTwitterV2BearerToken();
        if (null != bearerToken) {
            try {
                helper.setupRules(bearerToken, getRules());
                helper.connectStream(bearerToken);
            } catch (IOException | URISyntaxException e) {
                log.error("Error streaming tweets!", e);
                throw new RuntimeException("Error streaming tweets!", e);
            }
        } else {
            log.error("There was a problem getting your bearer token. " +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
            throw new RuntimeException("There was a problem getting your bearer token. +" +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
        }

    }

    private Map<String, String> getRules() {
        List<String> keywords = config.getTwitterKeywords();
        Map<String, String> rules = new HashMap<>();
        for (String keyword : keywords) {
            rules.put(keyword, "Keyword: " + keyword);
        }
        log.info("Created filter for twitter stream for keywords: {}", keywords);
        return rules;
    }
}
