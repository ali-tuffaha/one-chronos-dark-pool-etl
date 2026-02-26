package com.onechronos.darkpool.etl.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Validated Trade Record.
 */
public record CleanedTradeRecord(
        String tradeId,
        Instant timestampUtc,
        String symbol,
        Integer quantity,
        BigDecimal price,
        String buyerId,
        String sellerId,
        Boolean counterpartyConfirmed,
        Boolean discrepancyFlag
) {}