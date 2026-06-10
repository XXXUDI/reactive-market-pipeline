package com.socompany.datascraper.scraper;

import com.socompany.commonevents.mapper.AggTradeMapper;
import com.socompany.datascraper.producer.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import java.net.URI;

@Component
@Slf4j
@RequiredArgsConstructor
public class AggTradeScraper implements Scraper {

    private final WebSocketClient webSocketClient;
    private final String uri = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
    private final KafkaProducerService kafkaProducerService;

    @Override
    public void scrape() {
        log.info("Started scraping: AggTrade, symbol: BTCUSDT");

        webSocketClient.execute(
                URI.create(uri),
                session ->
                        session.receive()
                                .map(WebSocketMessage::getPayloadAsText)
                                .map(AggTradeMapper::map)
                                .flatMap(kafkaProducerService::sendRawTradeEvent)
                                .then()
        ).block();
    }
}
