package com.onechronos.darkpool.etl.model.enums;

/**
 * Enum to represent valid Sector values.
 */
public enum Sector {
    TECHNOLOGY("Technology"),
    CONSUMER_CYCLICAL("Consumer Cyclical"),
    AUTOMOTIVE("Automotive"),
    FINANCIAL_SERVICES("Financial Services"),
    INDUSTRIAL("Industrial");

    private final String displayName;

    Sector(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Parse Sector display name to Enum type.
     *
     * @param displayName to be parsed
     * @return Sector enum
     */
    public static Sector parse(String displayName) {
        if (displayName == null || displayName.isBlank()) {
            throw new IllegalArgumentException("Sector is null or blank");
        }
        for (Sector sector : values()) {
            if (sector.displayName.equalsIgnoreCase(displayName.trim())) {
                return sector;
            }
        }
        throw new IllegalArgumentException("Unknown sector: " + displayName);
    }

    public String getDisplayName() {
        return displayName;
    }
}