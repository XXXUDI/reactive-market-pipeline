package com.socompany.commonevents.raw;

import java.math.BigDecimal;

public record AggTradeEvent (
        String symbol,
        long aggTradeId,
        BigDecimal price,
        BigDecimal quantity,
        long tradeTimestamp,
        boolean isBuyerMarket
) {}
