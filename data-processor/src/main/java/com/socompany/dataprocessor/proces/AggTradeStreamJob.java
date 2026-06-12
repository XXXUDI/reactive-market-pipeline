package com.socompany.dataprocessor.proces;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.socompany.commonevents.enriched.EnrichedTrade;
import com.socompany.commonevents.raw.AggTradeEvent;
import com.socompany.commonevents.utils.TradeDirection;
import com.socompany.dataprocessor.config.KafkaConfiguration;
import com.socompany.dataprocessor.mapper.JsonToAggEventMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Component
public class AggTradeStreamJob implements CommandLineRunner {

    private final KafkaConfiguration kafkaConfig;
    private final ObjectMapper objectMapper;

    public AggTradeStreamJob(KafkaConfiguration kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting Flink AggTrade Stream Job...");
        log.info("Input topic: {}", kafkaConfig.getInputTopic());
        log.info("Output topic: {}", kafkaConfig.getOutputTopic());
        log.info("Bootstrap servers: {}", kafkaConfig.getBootstrapServers());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source для читання з топіку "market.agg-trade.raw"
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setTopics(kafkaConfig.getInputTopic())
                .setGroupId(kafkaConfig.getGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Створення потоку даних
        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Парсинг JSON в AggTradeEvent
        DataStream<AggTradeEvent> parsedStream = rawStream
                .map(new JsonToAggEventMapper())
                .name("Parse JSON to AggTradeEvent");

        // Групування по символу і обробка у віконному режимі (наприклад, 5 секунд)
        DataStream<EnrichedTrade> enrichedStream = parsedStream
                .keyBy(AggTradeEvent::symbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new TradeAggregator())
                .name("Aggregate and Enrich Trades");

        // Kafka Sink для запису в топік "market.agg-trade.enriched"
        KafkaSink<EnrichedTrade> kafkaSink = KafkaSink.<EnrichedTrade>builder()
                .setBootstrapServers(kafkaConfig.getBootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(kafkaConfig.getOutputTopic())
                                .setValueSerializationSchema(
                                        (element, context) -> objectMapper.writeValueAsBytes(element)
                                )
                                .build()
                )
                .build();

        enrichedStream.sinkTo(kafkaSink).name("Kafka Sink");

        log.info("Executing Flink job...");
        env.execute("AggTrade Stream Processing Job");
    }

    /**
     * Aggregator для обчислення VWAP, визначення тренду та створення EnrichedTrade
     */
    private static class TradeAggregator implements
            org.apache.flink.api.common.functions.AggregateFunction<AggTradeEvent, TradeAccumulator, EnrichedTrade> {

        @Override
        public TradeAccumulator createAccumulator() {
            return new TradeAccumulator();
        }

        @Override
        public TradeAccumulator add(AggTradeEvent trade, TradeAccumulator acc) {
            acc.symbol = trade.symbol();
            acc.totalQuantity = acc.totalQuantity.add(trade.quantity());
            acc.totalValue = acc.totalValue.add(trade.price().multiply(trade.quantity()));

            if (trade.isBuyerMarket()) {
                acc.buyVolume = acc.buyVolume.add(trade.quantity());
            } else {
                acc.sellVolume = acc.sellVolume.add(trade.quantity());
            }

            acc.lastPrice = trade.price();

            if (acc.windowStart == 0) {
                acc.windowStart = trade.tradeTimestamp();
            }
            acc.windowEnd = trade.tradeTimestamp();

            return acc;
        }

        @Override
        public EnrichedTrade getResult(TradeAccumulator acc) {
            // Обчислюємо VWAP (Volume-Weighted Average Price)
            BigDecimal vwap = acc.totalQuantity.compareTo(BigDecimal.ZERO) > 0
                    ? acc.totalValue.divide(acc.totalQuantity, 8, RoundingMode.HALF_UP)
                    : BigDecimal.ZERO;

            // Визначаємо напрямок тренду на основі співвідношення buy/sell обсягів
            TradeDirection direction = acc.buyVolume.compareTo(acc.sellVolume) >= 0
                    ? TradeDirection.BUY
                    : TradeDirection.SELL;

            return new EnrichedTrade(
                    acc.symbol,
                    acc.lastPrice,
                    acc.totalQuantity,
                    vwap,
                    acc.totalValue,
                    direction,
                    acc.windowStart,
                    acc.windowEnd
            );
        }

        @Override
        public TradeAccumulator merge(TradeAccumulator a, TradeAccumulator b) {
            a.totalQuantity = a.totalQuantity.add(b.totalQuantity);
            a.totalValue = a.totalValue.add(b.totalValue);
            a.buyVolume = a.buyVolume.add(b.buyVolume);
            a.sellVolume = a.sellVolume.add(b.sellVolume);
            a.lastPrice = b.lastPrice; // Беремо останню ціну з другого акумулятора
            a.windowEnd = Math.max(a.windowEnd, b.windowEnd);
            a.windowStart = Math.min(a.windowStart, b.windowStart);
            return a;
        }
    }

    /**
     * Accumulator to store data during aggregation
     */
    private static class TradeAccumulator {
        String symbol;
        BigDecimal totalQuantity = BigDecimal.ZERO;
        BigDecimal totalValue = BigDecimal.ZERO;
        BigDecimal buyVolume = BigDecimal.ZERO;
        BigDecimal sellVolume = BigDecimal.ZERO;
        BigDecimal lastPrice = BigDecimal.ZERO;
        long windowStart = 0;
        long windowEnd = 0;
    }
}
