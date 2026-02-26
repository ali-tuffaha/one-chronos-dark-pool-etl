package com.onechronos.darkpool.etl.config;

import java.nio.file.Path;

/**
 * Config record encapsulating output file paths
 *
 * @param cleanedTradesFile
 * @param exceptionsReportFile
 */
public record WriteConfig(
        Path cleanedTradesFile,
        Path exceptionsReportFile
) {
}
