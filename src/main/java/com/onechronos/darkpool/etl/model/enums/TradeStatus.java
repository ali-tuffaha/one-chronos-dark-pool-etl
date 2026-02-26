package com.onechronos.darkpool.etl.model.enums;

/**
 * Enum to represent valid trade status values.
 */
public enum TradeStatus {
    EXECUTED,
    CANCELLED;

    /**
     * Parse string to TradeStatus enum
     * @param value to be parsed
     * @return TradeStatus enum
     */
    public static TradeStatus parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Trade status is null or blank");
        }
        return switch (value.trim().toUpperCase()) {
            case "EXECUTED"  -> EXECUTED;
            case "CANCELLED" -> CANCELLED;
            default -> throw new IllegalArgumentException("Unknown trade status: " + value);
        };
    }
}