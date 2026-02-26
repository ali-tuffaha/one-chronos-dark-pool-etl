package com.onechronos.darkpool.etl.config;

import com.onechronos.darkpool.etl.exception.ConfigLoadException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Class for loading application config file from an external config file.
 */
public class AppConfigLoader {
    private static final Logger log = LoggerFactory.getLogger(AppConfigLoader.class);

    private AppConfigLoader() {
    }

    public static AppConfigLoader build() {
        return new AppConfigLoader();
    }

    public AppConfig load(Optional<Path> configFilePath) throws ConfigLoadException {
        try {
            log.info("Loading configuration from {}", configFilePath);

            Config config;

            if (configFilePath.isPresent()) {
                Path path = configFilePath.get();
                if (!path.toFile().exists()) {
                    throw new ConfigLoadException("Configuration file not found: %s".formatted(path));
                }
                log.info("Loading external configuration from {}", path);
                config = ConfigFactory.parseFile(path.toFile()).resolve();
            } else {
                log.info("No config file provided. Loading default config file: application.conf");
                config = ConfigFactory.load("application.conf").resolve();
            }

            AppConfig appConfig = new AppConfig(
                    parserReadConfig(config.getConfig("read-config")),
                    parseWriteConfig(config.getConfig("write-config")),
                    parserValidationConfig(config.getConfig("validation-config"))
            );

            log.info("Configuration loaded successfully");
            log.debug("Loaded config: {}", appConfig);

            return appConfig;
        } catch (IllegalArgumentException e) {
            throw new ConfigLoadException("Failed to parse field in %s".formatted(configFilePath), e);
        } catch (ConfigException e) {
            throw new ConfigLoadException("Failed to parse config file %s".formatted(configFilePath), e);
        }
    }


    private ReadConfig parserReadConfig(Config conf) {
        return new ReadConfig(
                getPath(conf, "symbols-ref-file"),
                getPath(conf, "fills-file"),
                getPath(conf, "trades-file")
        );
    }

    private WriteConfig parseWriteConfig(Config conf) {
        return new WriteConfig(
                getPath(conf, "cleaned-trades-file"),
                getPath(conf, "exceptions-report-file")
        );
    }

    private ValidationConfig parserValidationConfig(Config conf) {
        try {
            return new ValidationConfig(
                    new BigDecimal(conf.getString("price-discrepancy-threshold"))
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid price-discrepancy-threshold: " + e.getMessage());
        }
    }

    private Path getPath(Config conf, String key) {
        String path = conf.getString(key);
        try {
            return Path.of(path);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid Path %s at config key: %s".formatted(path, key));
        }
    }
}
