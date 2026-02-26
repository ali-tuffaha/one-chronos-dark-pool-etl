package com.onechronos.darkpool.etl;

import com.onechronos.darkpool.etl.cli.CliArgs;
import com.onechronos.darkpool.etl.cli.CliParser;
import com.onechronos.darkpool.etl.config.AppConfig;
import com.onechronos.darkpool.etl.config.AppConfigLoader;
import com.onechronos.darkpool.etl.exception.CliParseException;
import com.onechronos.darkpool.etl.exception.ConfigLoadException;
import com.onechronos.darkpool.etl.metrics.AppMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application Entry point.
 * - Start Metrics collection
 * - Parse Command Line arguments
 * - Load Config File
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try (AppMetrics appMetrics = AppMetrics.build()) {
            // Parse command line arguments
            CliArgs cliArgs = CliParser.build().parse(args);

            // Load config file
            AppConfig config = AppConfigLoader.build().load(cliArgs.configFilePath());

            log.info("CliArgs: {}", cliArgs);

            appMetrics.stopAppExecutionTime();
            appMetrics.printSummary();
        } catch (CliParseException e) {
            log.error("Exception thrown while parsing command line arguments", e);
        } catch (ConfigLoadException e) {
            log.error("Exception thrown while loading configs", e);
        }
    }
}