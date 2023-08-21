package dev.codingstoic.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;


@RequiredArgsConstructor
public class KafkaProducer {
    private final Producer<String, String> producer;

    public Future<RecordMetadata> send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic_sports_new", key, value);
        return producer.send(record);
    }

    public void flush() {
        producer.flush();
    }

    public void beginTransaction() {
        producer.beginTransaction();
    }

    public void initTransaction() {
        producer.initTransactions();
    }

    public void commitTransaction() {
        producer.commitTransaction();
    }
}
