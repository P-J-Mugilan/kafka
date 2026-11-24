package com.onrender.pjmugilan.spring_boot_kafka_producer.controller;


import com.onrender.pjmugilan.spring_boot_kafka_producer.service.ProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaProducerController {

    private final ProducerService producerService;

    public KafkaProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * REST endpoint for publishing messages to Kafka.
     */
    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(
            @RequestBody Map<String, String> request) {

        String message = request.get("message");

        if (message == null || message.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("Message cannot be empty");
        }

        producerService.sendMessage(message);

        return ResponseEntity.accepted()
                .body("Message submitted to Kafka successfully");
    }
}
