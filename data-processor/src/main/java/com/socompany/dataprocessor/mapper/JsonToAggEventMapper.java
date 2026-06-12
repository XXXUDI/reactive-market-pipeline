package com.socompany.dataprocessor.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.socompany.commonevents.raw.AggTradeEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToAggEventMapper implements MapFunction<String, AggTradeEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public AggTradeEvent map(String json) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(json, AggTradeEvent.class);
    }
}
