package com.onechronos.darkpool.etl.extract;

import com.onechronos.darkpool.etl.model.ExceptionRecord;

import java.util.Optional;


/**
 * The result of reading and mapping a single CSV row.
 * One only parsedRow or exception can be present at once.
 *
 * @param parsedRow successfully parsed row (if present then exception is not present)
 * @param exception exception record if parsing failed  (if present then value is not present)
 * @param <T>
 */
public record CsvReaderRowResult<T>(
        Optional<T> parsedRow,
        Optional<ExceptionRecord> exception
) {

    public boolean isSuccess() {
        return parsedRow.isPresent();
    }

    public static <T> CsvReaderRowResult<T> success(T value) {
        return new CsvReaderRowResult<>(Optional.of(value), Optional.empty());
    }

    public static <T> CsvReaderRowResult<T> failure(ExceptionRecord exception) {
        return new CsvReaderRowResult<>(Optional.empty(), Optional.of(exception));
    }
}
