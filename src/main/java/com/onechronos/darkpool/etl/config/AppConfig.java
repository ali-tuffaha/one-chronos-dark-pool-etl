package com.onechronos.darkpool.etl.config;

public record AppConfig(
        ReadConfig readConfig,
        WriteConfig writeConfig
) {
}
