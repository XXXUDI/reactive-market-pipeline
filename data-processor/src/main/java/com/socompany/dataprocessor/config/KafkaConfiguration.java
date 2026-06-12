package com.socompany.dataprocessor.config;

import com.socompany.commonkafka.topics.AggregationTradeTopics;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfiguration {
    private String bootstrapServers;
    private String inputTopic = AggregationTradeTopics.RAW;
    private String outputTopic = AggregationTradeTopics.ENRICHED;
    private String groupId = "market-processor";
}
