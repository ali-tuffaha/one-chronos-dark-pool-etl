package com.onechronos.darkpool.etl.exception;


import org.apache.commons.cli.ParseException;

/**
 * Thrown when the application fails to parse command line arguments.
 */
public class CliParseException extends Exception {
    public CliParseException(ParseException parseException) {
        super("Failed to parse command line arguments", parseException);
    }
}
