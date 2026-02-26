package com.onechronos.darkpool.etl.config;

import java.nio.file.Path;

public record WriteConfig(
        Path cleanedTradesFile,
        Path exceptionsReportFile
) {
}
