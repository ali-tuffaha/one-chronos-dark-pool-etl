package com.onechronos.darkpool.etl.exception;

/**
 * Thrown when the application fails to apply transformation on TradeRecord.
 */
public class TransformerException extends RuntimeException {

    public TransformerException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public TransformerException(String msg) {
        super(msg);
    }
}