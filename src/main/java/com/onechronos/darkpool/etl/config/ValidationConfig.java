package com.onechronos.darkpool.etl.config;

import java.math.BigDecimal;

public record ValidationConfig(
        BigDecimal priceDiscrepancyThreshold
) {
}