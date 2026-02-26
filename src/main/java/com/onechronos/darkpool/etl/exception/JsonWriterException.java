package com.onechronos.darkpool.etl.exception;

/**
 * Thrown when the application fails to write JSON file.
 */
public class JsonWriterException extends RuntimeException {

    public JsonWriterException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public JsonWriterException(String msg) {
        super(msg);
    }
}