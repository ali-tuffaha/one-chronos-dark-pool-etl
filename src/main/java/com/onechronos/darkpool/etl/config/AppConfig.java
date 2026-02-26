package com.onechronos.darkpool.etl.config;

/**
 * Config record encapsulating all application configs.
 *
 * @param readConfig
 * @param writeConfig
 */
public record AppConfig(
        ReadConfig readConfig,
        WriteConfig writeConfig,
        ValidationConfig validationConfig
) {
}
