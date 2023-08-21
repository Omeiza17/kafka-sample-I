package dev.codingstoic.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class KafkaProducerTest {

    private KafkaProducer producer;
    private MockProducer<String, String> mockProducer;

    private void buildMockProducer(boolean autoComplete) {
        this.mockProducer = new MockProducer<>(autoComplete, new StringSerializer(), new StringSerializer());
    }


    @Test
    @DisplayName("Verify History a key-value pair message is autocompleted")
    void givenKeyValue_whenSend_thenVerifyHistory() throws ExecutionException, InterruptedException {
        buildMockProducer(true);

        producer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> recordMetadataFuture = producer.send("data", "{\"site\" : \"codingstoic\"}");

        assertEquals(1, mockProducer.history().size());
        assertTrue(mockProducer.history().get(0).key().equalsIgnoreCase("data"));
        assertEquals(0, recordMetadataFuture.get().partition());
    }

    @Test
    @DisplayName("Send a key-value pair message only when flushed")
    void givenKeyValue_whenSend_thenSendOnlyAfterFlush() {

        buildMockProducer(false);
        //when
        producer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> record = producer.send("data", "{\"site\" : \"codingstoic\"}");
        assertFalse(record.isDone());

        //then
        producer.flush();
        assertTrue(record.isDone());
    }

    @Test
    @DisplayName("Return an Exception upon send given a key-value pair")
    void returnExceptionGivenKeyValue() {
        buildMockProducer(false);
        producer = new KafkaProducer(mockProducer);
        Future<RecordMetadata> record = producer.send("site", "{\"site\" : \"codingstoic\"}");

        var exception = new RuntimeException();

        mockProducer.errorNext(exception);

        try {
            record.get();
        } catch (ExecutionException | InterruptedException ex) {
            assertEquals(exception, ex.getCause());
        }
        assertTrue(record.isDone());
    }

    @Test
    @DisplayName("Only send transaction events when the events are committed")
    void sendWithTxnOnlyTxnCommit() {
        buildMockProducer(true);

        producer = new KafkaProducer(mockProducer);
        producer.initTransaction();
        producer.beginTransaction();
        producer.send("data", "{\"site\" : \"codingstoic\"}");

        assertTrue(mockProducer.history().isEmpty());
        producer.commitTransaction();
        assertEquals(1, mockProducer.history().size());
    }

}
