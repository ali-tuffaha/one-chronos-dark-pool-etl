package com.onechronos.darkpool.etl;

import com.onechronos.darkpool.etl.cli.CliArgs;
import com.onechronos.darkpool.etl.cli.CliParser;
import com.onechronos.darkpool.etl.exception.CliParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application Entry point.
 * - Parse Command Line arguments
 * - Load Config File
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            CliArgs cliArgs = CliParser.build().parse(args);

            log.info("CliArgs: {}", cliArgs);
        } catch (CliParseException e) {
            log.error("Exception thrown while parsing command line arguments", e);
        }
    }
}