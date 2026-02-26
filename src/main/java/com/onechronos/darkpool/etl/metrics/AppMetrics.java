package com.onechronos.darkpool.etl.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class AppMetrics implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AppMetrics.class);

    private final Timer appExecutionTime;
    private final Counter tradesRead;
    private final Counter tradesCancelled;
    private final Counter tradesCleanedWritten;
    private final Counter tradesExceptionWritten;
    private final Counter tradesParseFailed;
    private final Counter fillsRead;
    private final Counter fillsParseFailed;
    private final Counter symbolsRead;
    private final Counter symbolsParseFailed;

    private final Timer.Sample executionTime;

    SimpleMeterRegistry registry;

    private AppMetrics() {
        registry = new SimpleMeterRegistry();

        this.appExecutionTime = Timer.builder("app.execution.time")
                .description("Total elapsed time running app")
                .register(registry);

        executionTime = Timer.start(registry);

        this.tradesRead = Counter.builder("trades.read")
                .description("Total trade rows read from CSV")
                .register(registry);

        this.tradesCancelled = Counter.builder("trades.cancelled")
                .description("Trade rows skipped due to CANCELLED status")
                .register(registry);

        this.tradesCleanedWritten = Counter.builder("trades.cleaned.written")
                .description("Clean trades written to cleaned_trades.json")
                .register(registry);

        this.tradesExceptionWritten = Counter.builder("trades.exception.written")
                .description("Trade exception records written to exceptions_report.json")
                .register(registry);

        this.tradesParseFailed = Counter.builder("trades.parse.failed")
                .description("Trade rows that failed to parse")
                .register(registry);

        this.fillsRead = Counter.builder("fills.read")
                .description("Total fill rows read from CSV")
                .register(registry);

        this.fillsParseFailed = Counter.builder("fills.parse.failed")
                .description("Fill rows that failed to parse")
                .register(registry);

        this.symbolsRead = Counter.builder("symbols.read")
                .description("Total symbol rows read from CSV")
                .register(registry);

        this.symbolsParseFailed = Counter.builder("symbols.parse.failed")
                .description("Symbol rows that failed to parse")
                .register(registry);
    }

    public void stopAppExecutionTime() {
        executionTime.stop(appExecutionTime);
    }

    public void incrementTradesRead() {
        tradesRead.increment();
    }

    public void incrementTradesCancelled() {
        tradesCancelled.increment();
    }

    public void incrementTradesCleanedWritten() {
        tradesCleanedWritten.increment();
    }

    public void incrementTradesExceptionWritten() {
        tradesExceptionWritten.increment();
    }

    public void incrementTradesParseFailed() {
        tradesParseFailed.increment();
    }

    public void incrementFillsRead() {
        fillsRead.increment();
    }

    public void incrementFillsParsesFailed() {
        fillsParseFailed.increment();
    }

    public void incrementSymbolsRead() {
        symbolsRead.increment();
    }

    public void incrementSymbolsParsesFailed() {
        symbolsParseFailed.increment();
    }

    public void printSummary() {
        log.info("===== Pipeline Metrics =====");
        log.info("  Execution Time (MS)             : {}", (long) appExecutionTime.totalTime(TimeUnit.MILLISECONDS));
        log.info("  Trades Read             : {}", (long) tradesRead.count());
        log.info("  Trades Cancelled        : {}", (long) tradesCancelled.count());
        log.info("  Trades Cleaned Written  : {}", (long) tradesCleanedWritten.count());
        log.info("  Trades Exceptions Written : {}", (long) tradesExceptionWritten.count());
        log.info("  Trades Parse Failed    : {}", (long) tradesParseFailed.count());
        log.info("  Fills Read              : {}", (long) fillsRead.count());
        log.info("  Fills Parse Failed     : {}", (long) fillsParseFailed.count());
        log.info("  Symbols Read            : {}", (long) symbolsRead.count());
        log.info("  Symbols Parses Failed   : {}", (long) symbolsParseFailed.count());
        log.info("============================");
    }

    public static AppMetrics build() {
        return new AppMetrics();
    }

    @Override
    public void close() {
        log.info("Closing metrics...");
        registry.close();
    }
}
