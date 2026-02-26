package com.onechronos.darkpool.etl.exception;

import java.io.IOException;

/**
 * Thrown when the application fails to read and map a CSV file.
 */
public class CsvReaderException extends Exception {

    public CsvReaderException(String message) {
        super(message);
    }

    public CsvReaderException(String msg, IOException cause) {
        super(msg, cause);
    }
}