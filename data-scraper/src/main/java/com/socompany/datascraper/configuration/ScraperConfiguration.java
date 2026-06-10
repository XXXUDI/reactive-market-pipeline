package com.socompany.datascraper.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

@Configuration
public class ScraperConfiguration {

    @Bean
    public WebSocketClient webSocketClient() {
        return new ReactorNettyWebSocketClient();
    }
}
