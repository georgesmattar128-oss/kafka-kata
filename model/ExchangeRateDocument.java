package com.learn.kafka.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Map;

@Data
@Document(indexName = "exchange-rates")
public class ExchangeRateDocument {

    @Id
    private String id;

    @Field(name = "@timestamp", type = FieldType.Date)
    private String timestamp;

    @Field(type = FieldType.Keyword)
    private String base;

    @Field(type = FieldType.Object)
    private Map<String, Double> rates;
}