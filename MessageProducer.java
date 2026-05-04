package com.learn.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.learn.kafka.service.ExchangeRateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
public class MessageProducer {

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ExchangeRateService exchangeRateService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Scheduled(fixedRateString = "${exchange.fetch.interval}")
    public void fetchAndPublish() {
        log.info("Récupération des taux de change...");

        String rawRates = exchangeRateService.fetchRates();

        if (rawRates == null) {
            log.warn("Aucune donnée reçue, publication annulée.");
            return;
        }

        try {
            ObjectNode node = (ObjectNode) objectMapper.readTree(rawRates);
            node.put("@timestamp", Instant.now().toString());

            String message = objectMapper.writeValueAsString(node);
            kafkaTemplate.send(topic, message);

            log.info("Message publié sur le topic '{}' — {} devises",
                    topic,
                    node.path("rates").size());

        } catch (Exception e) {
            log.error("Erreur lors de la publication Kafka : {}", e.getMessage());
        }
    }

    public void sendMessage(String message) {
        kafkaTemplate.send(topic, message);
        log.info("Message manuel envoyé : {}", message);
    }
}