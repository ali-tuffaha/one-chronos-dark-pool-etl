package com.onechronos.darkpool.etl.config;

import java.nio.file.Path;

/**
 * Config record encapsulating input file paths
 *
 * @param symbolsRefFile
 * @param fillsFile
 * @param tradesFile
 */
public record ReadConfig(
        Path symbolsRefFile,
        Path fillsFile,
        Path tradesFile
) {
}
