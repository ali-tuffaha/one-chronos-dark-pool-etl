package com.onechronos.darkpool.etl.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Class for handling metrics collection using micrometer.
 */
public class AppMetrics implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AppMetrics.class);

    private final Timer appExecutionTime;
    private final Counter recordsRead;

    private final Timer.Sample executionTime;

    SimpleMeterRegistry registry;

    private AppMetrics() {
        registry = new SimpleMeterRegistry();

        this.appExecutionTime = Timer.builder("app.execution.time")
                .description("Total elapsed time running app")
                .register(registry);

        executionTime = Timer.start(registry);

        this.recordsRead = Counter.builder("records.read")
                .description("Total rows read from CSV")
                .register(registry);

    }

    public static AppMetrics build() {
        return new AppMetrics();
    }

    public void stopAppExecutionTime() {
        executionTime.stop(appExecutionTime);
    }

    public void incrementRecordsRead() {
        recordsRead.increment();
    }

    public void printSummary() {
        log.info("===== Pipeline Metrics =====");
        log.info("  Execution Time (MS)             : {}", (long) appExecutionTime.totalTime(TimeUnit.MILLISECONDS));
        log.info("  Records Read             : {}", (long) recordsRead.count());
        log.info("============================");
    }

    @Override
    public void close() {
        log.info("Closing metrics...");
        registry.close();
    }
}
