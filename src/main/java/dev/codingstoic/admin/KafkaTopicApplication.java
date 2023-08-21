package dev.codingstoic.admin;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@RequiredArgsConstructor
public class KafkaTopicApplication {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicApplication.class);

    private final static int PARTITION = 1;
    private final static short REPLICATION_FACTOR = 1;

    private final Properties properties;


    public void createTopic(String topicName) throws ExecutionException, InterruptedException {
        try (Admin admin = Admin.create(properties)){
            NewTopic newTopic = new NewTopic(topicName, PARTITION, REPLICATION_FACTOR);
            var result = admin.createTopics(Collections.singleton(newTopic));
            KafkaFuture<Void> future = result.values().get(topicName);
            future.get();

            admin.listTopics().listings().whenComplete((topicListings, throwable) -> {
                topicListings.forEach(listing -> logger.info("Listing: {}", listing));
            });
        }
    }

}
