package dev.codingstoic.admin;


import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

@Testcontainers
public class KafkaTopicApplicationTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    private KafkaTopicApplication kafkaTopicApplication;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        kafkaTopicApplication = new KafkaTopicApplication(properties);
    }

    @Test
    void testCreateTopic() throws ExecutionException, InterruptedException, IOException {
        String topicName = "test-topic";
        kafkaTopicApplication.createTopic(topicName);

        String topicCommand = "/usr/bin/kafka-topics --bootstrap-server=localhost:9092 --list";
        String output = KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", topicCommand)
                .getStdout();

        assertThat(output, containsString(topicName));
    }
}
