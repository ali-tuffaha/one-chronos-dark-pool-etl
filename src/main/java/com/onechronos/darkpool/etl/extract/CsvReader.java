package com.onechronos.darkpool.etl.extract;

import com.onechronos.darkpool.etl.exception.CsvReaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Generic streaming CSV reader.
 */
public class CsvReader {
    private static final Logger log = LoggerFactory.getLogger(CsvReader.class);

    private CsvReader() { }

    public static CsvReader build() {
        return new CsvReader();
    }

    private record LineWithNumber(long lineNumber, String line) {}


    /**
     * Opens the given CSV file and returns a lazy stream of parsed records.
     * @param path of csv file
     * @param mapper  functions that maps a row map into a CsvReaderRowResult
     * @return  lazy stream of ParseResult
     * @param <T> result type
     * @throws CsvReaderException if error occurs while reading or mapping csv file
     */
    public <T> Stream<CsvReaderRowResult<T>> readFile(
            Path path,
            Function<CsvRow, CsvReaderRowResult<T>> mapper
    ) throws CsvReaderException {
        try {
            log.info("Opening CSV file: {}", path);

            BufferedReader reader = Files.newBufferedReader(path);

            String headerLine = reader.readLine();
            if (Objects.isNull(headerLine)) {   // readLine returns null only if EOF is reached.
                reader.close();
                throw new CsvReaderException("Empty CSV file: %s".formatted(path));
            }

            String[] headers = parseLine(headerLine);
            log.debug("Headers for {}: {}", path.getFileName(), List.of(headers));

            // Simple counter for line number
            AtomicInteger lineNumber = new AtomicInteger(2);

            return reader.lines()
                    .peek(line -> log.debug("Processing Row {}: {}", lineNumber.get(), line))
                    .map(line -> new LineWithNumber(lineNumber.getAndIncrement(), line))
                    .filter(lineWithNumber -> shouldProcessLine(lineWithNumber.line(), path))
                    .map(l -> new CsvRow(l.lineNumber(), toRow(headers, l.line())))
                    .map(mapper)
                    .onClose(() -> closeFile(reader, path));

        } catch (IOException e) {
            throw new CsvReaderException("Failed to read CSV file: %s".formatted(path), e);
        }
    }

    /**
     * Checks if a line is empty.
     */
    private boolean shouldProcessLine(String line, Path path) {
        if (!line.isBlank()) return true;
        log.debug("Skipping empty line in {}", path.getFileName());
        return false;
    }

    /**
     * Maps a header array and a CSV line into a column-keyed map.
     */
    private Map<String, String> toRow(String[] headers, String line) {
        Map<String, String> row = new HashMap<>();
        String[] values = parseLine(line);

        for (int i = 0; i < headers.length; i++) {
            String value = (i < values.length) ? values[i].trim() : null;
            row.put(headers[i], (Objects.isNull(value) || value.isEmpty()) ? null : value);
        }

        return row;
    }

    /**
     * Splits a CSV line into tokens, respecting double-quoted fields that may contain commas.
     */
    private String[] parseLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                tokens.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        tokens.add(current.toString().trim());

        return tokens.toArray(new String[0]);
    }

    /**
     * Close csv safely.
     */
    private void closeFile(BufferedReader reader, Path path) {
        try {
            log.debug("Closing CSV file: {}", path);
            reader.close();
        } catch (IOException e) {
            log.warn("Failed to close reader for {}", path, e);
        }
    }
}
