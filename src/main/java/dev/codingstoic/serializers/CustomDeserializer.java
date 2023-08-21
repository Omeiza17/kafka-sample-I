package dev.codingstoic.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.codingstoic.dto.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class CustomDeserializer implements Deserializer<Message> {

    private static final Logger logger = LoggerFactory.getLogger(CustomDeserializer.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                logger.info("Null received while deserializing");
                return null;
            }
            logger.info("Deserializing...");
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), Message.class);
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Error when deserializing byte[] to MessageDto");
        }
    }

}
