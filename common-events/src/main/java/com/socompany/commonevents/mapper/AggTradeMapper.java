package com.socompany.commonevents.mapper;

import com.socompany.commonevents.raw.AggTradeEvent;

import java.math.BigDecimal;
import java.time.Instant;

public class AggTradeMapper {

    public static AggTradeEvent map(String data) {
        // {"e":"trade","E":1779575491864,"s":"BTCUSDT","t":6317848782,"p":"76511.89000000","q":"0.00007000","T":1779575491864,"m":true,"M":true}

        //String eventType = extractString(data, "\"e\":");
        //Long eventTimeMs = extractLong(data, "\"E\":");
        String symbol = extractString(data, "\"s\":");
        Long aggregateTradeId = extractLong(data, "\"t\":");
        BigDecimal price = extractBigDecimal(data, "\"p\":");
        BigDecimal quantity = extractBigDecimal(data, "\"q\":");
        //Long firstTradeId = extractLong(data, "\"f\":");
        //Long lastTradeId = extractLong(data, "\"l\":");
        Long tradeTimeMs = extractLong(data, "\"T\":");
        boolean buyerIsMaker = extractBoolean(data, "\"m\":");

        return new AggTradeEvent(
                symbol,
                aggregateTradeId,
                price,
                quantity,
                tradeTimeMs,
                buyerIsMaker
        );
    }

    private static String extractString(String data, String key) {
        int start = data.indexOf(key);
        if (start == -1) return null;
        start = data.indexOf("\"", start + key.length()) + 1;
        int end = data.indexOf("\"", start);
        return data.substring(start, end);
    }

    private static Long extractLong(String data, String key) {
        int start = data.indexOf(key);
        if (start == -1) return null;
        start = start + key.length();
        int end = data.indexOf(",", start);
        if (end == -1) end = data.indexOf("}", start);
        return Long.parseLong(data.substring(start, end).trim());
    }

    private static BigDecimal extractBigDecimal(String data, String key) {
        int start = data.indexOf(key);
        if (start == -1) return null;
        start = data.indexOf("\"", start + key.length()) + 1;
        int end = data.indexOf("\"", start);
        return new BigDecimal(data.substring(start, end));
    }

    private static boolean extractBoolean(String data, String key) {
        int start = data.indexOf(key);
        if (start == -1) return false;
        start = start + key.length();
        int end = data.indexOf(",", start);
        if (end == -1) end = data.indexOf("}", start);
        return Boolean.parseBoolean(data.substring(start, end).trim());
    }
}
