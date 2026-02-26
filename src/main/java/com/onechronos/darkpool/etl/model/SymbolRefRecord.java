package com.onechronos.darkpool.etl.model;

import com.onechronos.darkpool.etl.model.enums.Sector;

/**
 * Type record to represent a symbol reference row.
 */
public record SymbolRefRecord(
        String symbol,
        String companyName,
        Sector sector,
        Boolean isActive
) {
}