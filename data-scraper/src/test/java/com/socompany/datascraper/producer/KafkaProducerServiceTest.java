package com.socompany.datascraper.producer;

import com.socompany.commonevents.raw.AggTradeEvent;
import com.socompany.commonkafka.topics.AggregationTradeTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {AggregationTradeTopics.RAW})
class KafkaProducerServiceTest {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @DynamicPropertySource
    static void modifyProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> "${spring.embedded.kafka.brokers}");
    }

    @Test
    void shouldSendRealJsonToKafka() {
        AggTradeEvent event = new AggTradeEvent(
                "ETHUSDT",
                9999L,
                new BigDecimal("1500.00"),
                new BigDecimal("1.5"),
                System.currentTimeMillis(),
                false
        );

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        consumerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Зчитуємо як String, щоб перевірити, чи там дійсно валідний JSON
        DefaultKafkaConsumerFactory<String, String> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());

        var consumer = consumerFactory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, AggregationTradeTopics.RAW);

        // 3. Викликаємо сервіс і перевіряємо завершення потоку відправки
        StepVerifier.create(kafkaProducerService.sendRawTradeEvent(event))
                .verifyComplete();

        // 4. Вичитуємо повідомлення з топіка брокера і робимо асерти
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, AggregationTradeTopics.RAW);

        assertThat(singleRecord).isNotNull();
        assertThat(singleRecord.key()).isEqualTo("ETHUSDT:9999");
        // Перевіряємо, чи є в JSON-рядку наші поля
        assertThat(singleRecord.value()).contains("\"symbol\":\"ETHUSDT\"");
        assertThat(singleRecord.value()).contains("\"aggTradeId\":9999");
        assertThat(singleRecord.value()).contains("\"price\":1500");

        consumer.close();
    }
}