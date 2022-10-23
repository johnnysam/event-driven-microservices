package com.microservices.twitter.to.kafka.service.runner;

import com.microservices.twitter.to.kafka.service.config.TwitterToKafkaConfig;
import com.microservices.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnExpression("not ${twitter-to-kafka-service.enable-mock-tweets} && not ${twitter-to-kafka-service.enable-v2-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner{

    private final TwitterKafkaStatusListener listener;
    private final TwitterToKafkaConfig config;
    private TwitterStream twitterStream;

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);

        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            log.info("Shutting down twitter stream");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        var keywords = config.getTwitterKeywords().toArray(new String[0]);
        var filterQuery = new FilterQuery(keywords);

        twitterStream.filter(filterQuery);

        log.info("Started filtering twitter stream from the keywords {}", Arrays.asList(keywords));
    }
}
