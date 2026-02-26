package com.onechronos.darkpool.etl.model;

import java.util.Map;

/**
 * A record that failed one or more validation checks.
 */
public record ExceptionRecord(
        String recordId,
        String sourceFile,
        String exceptionType,
        String details,
        Map<String, String> rawData
) {
}