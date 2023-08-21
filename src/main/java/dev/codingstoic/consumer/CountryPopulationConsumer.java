package dev.codingstoic.consumer;


import dev.codingstoic.dto.CountryPopulation;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.StreamSupport;


@RequiredArgsConstructor
public class CountryPopulationConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CountryPopulationConsumer.class);

    private Consumer<String, Integer> consumer;
    public java.util.function.Consumer<Throwable> exceptionConsumer;
    public java.util.function.Consumer<CountryPopulation> countryPopulationConsumer;

    void startBySubscribing(String topic) {
        consume(() -> consumer.subscribe(Collections.singleton(topic)));
    }

    private void consume(Runnable beforePollingTask) {
        try {
            beforePollingTask.run();
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(1000));
                StreamSupport.stream(records.spliterator(), false).map(record -> new CountryPopulation(record.key(),
                        record.value())).forEach(countryPopulationConsumer);
                consumer.commitSync();
            }
        } catch (WakeupException ex) {
            logger.info("Shutting down...");
        } catch (RuntimeException ex) {
            exceptionConsumer.accept(ex);
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        consumer.wakeup();
    }
}
