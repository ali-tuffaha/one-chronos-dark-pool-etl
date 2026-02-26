package com.onechronos.darkpool.etl;

import com.onechronos.darkpool.etl.config.AppConfig;
import com.onechronos.darkpool.etl.exception.CsvReaderException;
import com.onechronos.darkpool.etl.extract.CsvMappers;
import com.onechronos.darkpool.etl.extract.CsvReader;
import com.onechronos.darkpool.etl.extract.CsvRow;
import com.onechronos.darkpool.etl.load.JsonWriter;
import com.onechronos.darkpool.etl.metrics.AppMetrics;
import com.onechronos.darkpool.etl.model.FillRecord;
import com.onechronos.darkpool.etl.model.SymbolRefRecord;
import com.onechronos.darkpool.etl.model.TradeRecord;
import com.onechronos.darkpool.etl.model.enums.TradeStatus;
import com.onechronos.darkpool.etl.transform.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

public class Pipeline {
    private final static Logger log = LoggerFactory.getLogger(Pipeline.class);

    /**
     * Runs the full ETL pipeline:
     * - Loads symbol reference and fill data eagerly into memory
     * - Streams trades.csv, routing each row to one of:
     *      - valid trades, with discrepancy flag set if fill mismatches to cleaned trades output file
     *      - parse failures, duplicates, invalid/inactive symbols, fill mismatches exception report output file
     *      - Note: Cancelled trades are filtered silently and not written to either output
     * @param csvReader streaming CSV reader
     * @param config application configuration
     * @param metrics counters incremented throughout the pipeline
     */
    public static void runPipeline(
            CsvReader csvReader,
            AppConfig config,
            AppMetrics metrics
    ) throws CsvReaderException, IOException {

        // Load reference data eagerly
        Map<String, SymbolRefRecord> symbolMap = loadSymbolsMap(csvReader, config, metrics);
        Map<String, FillRecord>      fillMap   = loadFillsMap(csvReader, config, metrics);
        log.info("Loaded {} symbols, {} fills", symbolMap.size(), fillMap.size());

        Transformer transformer = Transformer.build(config.validationConfig(), symbolMap, fillMap);

        final Path tradesFile = config.readConfig().tradesFile();

        try (
                var tradeStream = csvReader.readFile(tradesFile, (CsvRow row) -> CsvMappers.toTradeRecord(row, tradesFile));
                var writer      = JsonWriter.open(config.writeConfig().cleanedTradesFile(), config.writeConfig().exceptionsReportFile())
        ) {
            log.info("Processing trade records....");
            tradeStream
                    .peek(r -> metrics.incrementTradesRead())
                    // Parse failures → exceptions
                    .peek(r -> r.exception().ifPresent(e -> {
                        log.debug("Parse failure: {}", e.details());
                        metrics.incrementTradesParseFailed();
                        writer.writeException(e);
                    }))
                    .filter(r -> r.parsedRow().isPresent())
                    .map(r -> r.parsedRow().get())
                    // Cancelled trades → skip with debug log
                    .filter(trade -> notCancelledTrade(metrics, trade))
                    // Transform — dedup, symbol validation, discrepancy flag
                    .map((TradeRecord trade) -> transformer.transform(trade, tradesFile))
                    // Route to output files
                    .forEach(result -> {
                        result.cleanedTrade().ifPresent(trade -> {
                            metrics.incrementTradesCleanedWritten();
                            writer.writeCleanedTrade(trade);
                        });
                        result.exception().ifPresent(e -> {
                            metrics.incrementTradesExceptionWritten();
                            writer.writeException(e);
                        });
                    });
            log.info("Trade records processing complete.");
        }
    }

    /**
     * Loads symbols_reference.csv into a map keyed by symbol (uppercased).
     * Rows that fail to parse are logged and skipped.
     */
    private static Map<String, SymbolRefRecord> loadSymbolsMap(
            CsvReader csvReader,
            AppConfig config,
            AppMetrics metrics
    ) throws CsvReaderException {
        final Path symbolsRefFile = config.readConfig().symbolsRefFile();
        try (var stream = csvReader.readFile(symbolsRefFile, (CsvRow row) -> CsvMappers.toSymbolRefRecord(row, symbolsRefFile))) {
            return stream
                    .peek(r -> metrics.incrementSymbolsRead())
                    .peek(r -> r.exception().ifPresent(e -> {
                        metrics.incrementSymbolsParsesFailed();
                        log.debug("Skipping invalid symbol row: {}", e.details());
                    }))
                    .filter(r -> r.parsedRow().isPresent())
                    .map(r -> r.parsedRow().get())
                    .collect(Collectors.toMap(
                            SymbolRefRecord::symbol,
                            s -> s
                    ));
        }
    }

    /**
     * Loads counterparty_fills.csv into a map keyed by ourTradeId.
     * Rows that fail to parse are logged and skipped.
     */
    private static Map<String, FillRecord> loadFillsMap(
            CsvReader csvReader,
            AppConfig config,
            AppMetrics metrics
    ) throws CsvReaderException {
        final Path fillsFile = config.readConfig().fillsFile();
        try (var stream = csvReader.readFile(fillsFile, (CsvRow row) -> CsvMappers.toFillRecord(row, fillsFile))) {
            return stream
                    .peek(r -> metrics.incrementFillsRead())
                    .peek(r -> r.exception().ifPresent(e -> {
                        metrics.incrementFillsParsesFailed();
                        log.debug("Skipping invalid fill row: {}", e.details());
                    }))
                    .filter(r -> r.parsedRow().isPresent())
                    .map(r -> r.parsedRow().get())
                    .collect(Collectors.toMap(
                            FillRecord::ourTradeId,
                            f -> f
                    ));
        }
    }

    private static boolean notCancelledTrade(AppMetrics metrics, TradeRecord trade) {
        if (trade.tradeStatus() == TradeStatus.CANCELLED) {
            log.debug("Skipping cancelled trade: {}", trade.tradeId());
            metrics.incrementTradesCancelled();
            return false;
        }
        return true;
    }
}
