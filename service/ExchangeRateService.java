package com.learn.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
@Service
public class ExchangeRateService {

    @Value("${exchange.api.url}")
    private String apiUrl;

    private final WebClient webClient;

    public ExchangeRateService(WebClient.Builder builder) {
        this.webClient = builder.build();
    }
    public String fetchRates() {
        try {
            String response = webClient
                    .get()
                    .uri(apiUrl)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            log.info("Taux récupérés depuis l'API externe");
            return response;

        } catch (Exception e) {
            log.error("Erreur lors de l'appel à l'API externe : {}", e.getMessage());
            return null;
        }
    }
}