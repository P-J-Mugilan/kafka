package com.onrender.pjmugilan.spring_boot_kafka_producer.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ProducerService {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;

    // Constructor injection for better testability and immutability.
    public ProducerService(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic}") String topicName) {

        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    /**
     * Publishes a message to the configured Kafka topic.
     * Handles asynchronous success and failure responses.
     */
    public CompletableFuture<SendResult<String, String>> sendMessage(
            String message) {

        return kafkaTemplate.send(topicName, message)
                .whenComplete((result, exception) -> {

                    if (exception != null) {
                        log.error(
                                "Failed to publish message to topic: {}",
                                topicName,
                                exception
                        );
                    } else {
                        log.info(
                                "Message published successfully | Topic: {} | Partition: {} | Offset: {}",
                                topicName,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset()
                        );
                    }
                });
    }
}

