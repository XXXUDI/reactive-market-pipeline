package com.socompany.commonevents.enriched;

import com.socompany.commonevents.utils.TradeDirection;

import java.math.BigDecimal;

public record EnrichedTrade (
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        BigDecimal vwap,                    // volume-weighted average price over a window
        BigDecimal totalPrice,              // sum of quantity in a window
        TradeDirection direction,           // BUY or SELL
        long windowStart,
        long windowEnd
) {}
