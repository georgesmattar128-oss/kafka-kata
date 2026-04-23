package com.learn.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRateDocument;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class MessageConsumer {

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message) {
        log.info("Message reçu depuis Kafka");

        try {
            Map<String, Object> data = objectMapper.readValue(message, Map.class);

            ExchangeRateDocument document = new ExchangeRateDocument();
            document.setTimestamp((String) data.get("@timestamp"));
            document.setBase((String) data.get("base"));
            document.setRates((Map<String, Double>) data.get("rates"));

            elasticsearchOperations.save(document);

            log.info("Document indexé dans Elasticsearch — base: {} | devises: {}",
                    document.getBase(),
                    document.getRates().size());

        } catch (Exception e) {
            log.error("Erreur lors de l'indexation Elasticsearch : {}", e.getMessage());
        }
    }
}