package com.socompany.dataprocessor.config;

import com.socompany.commonevents.raw.AggTradeEvent;
import com.socompany.commonkafka.kafkaFactories.KafkaFactories;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Bean
    ProducerFactory<String, AggTradeEvent> producerFactory() {
        return KafkaFactories.producerFactory("localhost:9092");
    }

    @Bean
    KafkaTemplate<String, AggTradeEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
