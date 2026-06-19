package com.socompany.dataprocessor.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.socompany.commonevents.enriched.EnrichedTrade;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.Serial;

public final class EnrichedTradeSerializationSchema implements SerializationSchema<EnrichedTrade> {

    @Serial
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serialize(EnrichedTrade element) {
        try {
            return getObjectMapper().writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing EnrichedTrade", e);
        }
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }
}