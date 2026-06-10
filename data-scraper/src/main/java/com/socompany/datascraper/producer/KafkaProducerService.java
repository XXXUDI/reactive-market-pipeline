package com.socompany.datascraper.producer;

import com.socompany.commonevents.raw.AggTradeEvent;
import com.socompany.commonkafka.topics.AggregationTradeTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {
    private final KafkaSender<String, AggTradeEvent> kafkaSender;
    private final String topic = AggregationTradeTopics.RAW;

    public Mono<Void> sendRawTradeEvent(AggTradeEvent aggTradeEvent) {
        String key = aggTradeEvent.symbol() +":"+ aggTradeEvent.aggTradeId();

        ProducerRecord<String, AggTradeEvent> record = new ProducerRecord<>(topic, key, aggTradeEvent);

        SenderRecord<String, AggTradeEvent, Long> senderRecord =
                SenderRecord.create(record, aggTradeEvent.aggTradeId());

        return kafkaSender.send(Mono.just(senderRecord))
                .doOnNext(result -> log.info("Trade event {} was successfully sent to topic {} with offset {}",
                        result.correlationMetadata(),
                        result.recordMetadata().topic(),
                        result.recordMetadata().offset()
                        )
                )
                .doOnError(error -> log.error("Error occurred while sending trade event to topic {}", topic, error))
                .then();
    }
}
