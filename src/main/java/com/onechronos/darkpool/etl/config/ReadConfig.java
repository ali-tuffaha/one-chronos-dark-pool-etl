package com.onechronos.darkpool.etl.config;

import java.nio.file.Path;

public record ReadConfig(
        Path symbolsRefFile,
        Path fillsFile,
        Path tradesFile
) {
}
