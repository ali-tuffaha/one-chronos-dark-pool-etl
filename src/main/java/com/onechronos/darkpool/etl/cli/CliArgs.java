package com.onechronos.darkpool.etl.cli;

import java.nio.file.Path;

/**
 * Record for Command Line Args
 *
 * @param configFilePath path for HOCON config file
 */
public record CliArgs(Path configFilePath) {
}

