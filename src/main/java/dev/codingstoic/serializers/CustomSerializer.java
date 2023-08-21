package dev.codingstoic.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.codingstoic.dto.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomSerializer implements Serializer<Message> {

    private static final Logger logger = LoggerFactory.getLogger(CustomSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String s, Message message) {
        try {
            if (message == null){
                logger.info("Null received at serializing");
                return null;
            }
            logger.info("Serializing...");
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Error when serializing Message to byte[]");
        }
    }
}
