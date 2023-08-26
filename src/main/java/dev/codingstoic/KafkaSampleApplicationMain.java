package dev.codingstoic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSampleApplicationMain {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSampleApplicationMain.class);
    public static void main(String[] args) {
        logger.info("Welcome to a Kafka sample project...!");

        SimpleConsumer simpleConsumer = new SimpleConsumer();

        simpleConsumer.consume("/Users/sm-omeiza/Dev/kafka-sample-I/src/main/resources/client.properties");
    }
}
