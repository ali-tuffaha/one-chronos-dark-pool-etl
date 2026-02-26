package com.onechronos.darkpool.etl.model;

import com.onechronos.darkpool.etl.model.enums.TradeStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Type record to represent a trade record.
 */
public record TradeRecord(
        String tradeId,
        Instant timestamp,
        String symbol,
        Integer quantity,
        BigDecimal price,
        String buyerId,
        String sellerId,
        TradeStatus tradeStatus,
        Map<String, String> rawData
) {
}