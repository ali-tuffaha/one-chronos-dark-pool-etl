package com.onechronos.darkpool.etl.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Type record to represent a fill record row.
 */
public record FillRecord(
        String externalRefId,
        String ourTradeId,
        Instant timestamp,
        String symbol,
        Integer quantity,
        BigDecimal price,
        String counterpartyId
) {}