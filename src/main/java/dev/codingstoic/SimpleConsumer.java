package dev.codingstoic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class SimpleConsumer {

    public void consume(String configFile) {
        if (configFile.isEmpty()) {
            log.error("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        final String topic = "pokemon";

        final Optional<Properties> props = SimpleConsumer.loadConfig(configFile);

        props.ifPresent(properties -> {
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "confluent-kafka");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        });

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props.get())) {
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    var key = record.key();
                    var value = record.value();
                    log.info("Consumed event from topic {} :: key = {} :: value = {}", topic, key, value);
                }
            }
        }

    }

    public static Optional<Properties> loadConfig(final String configFile) {
        if (!Files.exists(Paths.get(configFile))) {
            return Optional.empty();
        }
        try (InputStream inputStream = new FileInputStream(configFile)) {
            final Properties cfg = new Properties();
            cfg.load(inputStream);
            return Optional.of(cfg);
        } catch (IOException e) {
            log.error("Failed to load configuration file: {}", configFile, e);
            return Optional.empty();
        }
    }

}
