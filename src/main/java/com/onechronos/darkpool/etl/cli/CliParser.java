package com.onechronos.darkpool.etl.cli;


import com.onechronos.darkpool.etl.exception.CliParseException;
import org.apache.commons.cli.*;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.commons.cli.help.TextHelpAppendable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Class for Parsing command line arguments into CliArgs object.
 */
public class CliParser {
    private static final Logger log = LoggerFactory.getLogger(CliParser.class);

    private static final String CONFIG_FILE_PATH_SHORT_OPT = "c";

    private final Options options;

    private CliParser() {
        this.options = buildOptions();
    }

    private Options buildOptions() {
        Options opts = new Options();

        opts.addOption(Option.builder(CONFIG_FILE_PATH_SHORT_OPT)
                .longOpt("config-file-path")
                .desc("Path to the configuration file (required)")
                .hasArg()
                .argName("FILE")
                .required()
                .get());

        return opts;
    }

    public CliArgs parse(String[] args) throws CliParseException {
        try {
            log.info("Parsing command line args...");

            CommandLine cmd = new DefaultParser().parse(options, args);

            CliArgs cliArgs = new CliArgs(Path.of(cmd.getOptionValue(CONFIG_FILE_PATH_SHORT_OPT)));

            log.info("Command line args parsed {}", cliArgs);

            return cliArgs;
        } catch (ParseException e) {
            printHelp();
            throw new CliParseException(e);
        }
    }

    private void printHelp() {
        try {
            TextHelpAppendable out = new TextHelpAppendable(System.out);
            out.setMaxWidth(100);

            HelpFormatter formatter = HelpFormatter.builder()
                    .setHelpAppendable(out)
                    .get();

            formatter.printHelp(
                    "java -jar path/to/jar",
                    "Command Line Args:",
                    options,
                    "Example:\n  java -jar path/to/jar -c /etc/myapp/config.yaml",
                    true
            );
        } catch (IOException e) {
            log.error("Failed to print help", e);
        }
    }

    public static CliParser build() {
        return new CliParser();
    }
}
