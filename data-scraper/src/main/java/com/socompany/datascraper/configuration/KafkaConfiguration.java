package com.socompany.datascraper.configuration;

import com.socompany.commonevents.raw.AggTradeEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;


    // TODO: The producer serializers are swapped/incorrect for the declared KafkaSender<String, AggTradeEvent>. The key serializer should be StringSerializer and the value serializer should be JsonSerializer (for AggTradeEvent). With the current config, keys will not serialize as String and values will not serialize as JSON.
    @Bean
    public KafkaSender<String, AggTradeEvent> kafkaSender() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, AggTradeEvent> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }

}
