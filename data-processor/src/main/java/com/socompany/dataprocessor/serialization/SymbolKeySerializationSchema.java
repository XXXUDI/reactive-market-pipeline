package com.socompany.dataprocessor.serialization;

import com.socompany.commonevents.enriched.EnrichedTrade;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.Serial;

public final class SymbolKeySerializationSchema implements SerializationSchema<EnrichedTrade> {

    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(EnrichedTrade element) {
        return element.symbol().getBytes();
    }
}
