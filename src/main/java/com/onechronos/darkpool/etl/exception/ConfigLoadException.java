package com.onechronos.darkpool.etl.exception;

/**
 * Thrown when failing to read and load application config file.
 */
public class ConfigLoadException extends Exception {

    public ConfigLoadException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ConfigLoadException(String msg) {
        super(msg);
    }
}