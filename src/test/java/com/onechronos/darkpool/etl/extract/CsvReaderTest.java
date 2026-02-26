package com.onechronos.darkpool.etl.extract;

import com.onechronos.darkpool.etl.exception.CsvReaderException;
import com.onechronos.darkpool.etl.model.ExceptionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CsvReaderTest {

    @TempDir
    Path tempDir;

    private CsvReader csvReader;

    @BeforeEach
    void setUp() {
        csvReader = CsvReader.build();
    }


    @Test
    void parsesRowsCorrectly() throws IOException, CsvReaderException {
        Path csv = writeCsv("""
                id,name,value
                1,foo,100
                2,bar,200
                """);

        List<CsvReaderRowResult<Map<String, String>>> results;
        try (var stream = csvReader.readFile(csv, row -> CsvReaderRowResult.success(row.data()))) {
            results = stream.toList();
        }

        assertThat(results).hasSize(2);
        assertThat(results.get(0).parsedRow()).isPresent();
        assertThat(results.get(0).parsedRow().get()).containsEntry("id", "1").containsEntry("name", "foo");
        assertThat(results.get(1).parsedRow().get()).containsEntry("id", "2").containsEntry("name", "bar");
    }

    @Test
    void assignsCorrectLineNumbers() throws IOException, CsvReaderException {
        Path csv = writeCsv("""
                id,name
                1,foo
                2,bar
                """);

        List<CsvRow> rows;
        try (var stream = csvReader.readFile(csv, CsvReaderRowResult::success)) {
            rows = stream.map(r -> r.parsedRow().get()).toList();
        }

        assertThat(rows.get(0).rowNumber()).isEqualTo(2L);
        assertThat(rows.get(1).rowNumber()).isEqualTo(3L);
    }

    @Test
    void skipsBlankLines() throws IOException, CsvReaderException {
        Path csv = writeCsv("""
                id,name
                1,foo

                2,bar
                """);

        List<CsvReaderRowResult<Map<String, String>>> results;
        try (var stream = csvReader.readFile(csv, row -> CsvReaderRowResult.success(row.data()))) {
            results = stream.toList();
        }

        assertThat(results).hasSize(2);
    }

    @Test
    void handlesQuotedFieldsWithCommas() throws IOException, CsvReaderException {
        Path csv = writeCsv("""
                id,name
                1,"foo, bar"
                """);

        List<CsvReaderRowResult<Map<String, String>>> results;
        try (var stream = csvReader.readFile(csv, row -> CsvReaderRowResult.success(row.data()))) {
            results = stream.toList();
        }

        assertThat(results.get(0).parsedRow().get()).containsEntry("name", "foo, bar");
    }

    @Test
    void throwsOnEmptyFile() throws IOException {
        Path csv = writeCsv("");

        assertThatThrownBy(() -> {
            try (var stream = csvReader.readFile(csv, row -> CsvReaderRowResult.success(row.data()))) {
                stream.toList();
            }
        }).isInstanceOf(CsvReaderException.class)
          .hasMessageContaining("Empty CSV file");
    }

    @Test
    void mapsNullForMissingFields() throws IOException, CsvReaderException {
        Path csv = writeCsv("""
                id,name,value
                1,foo
                """);

        List<CsvReaderRowResult<Map<String, String>>> results;
        try (var stream = csvReader.readFile(csv, row -> CsvReaderRowResult.success(row.data()))) {
            results = stream.toList();
        }

        assertThat(results.get(0).parsedRow().get().get("value")).isNull();
    }

    @Test
    void isSuccessAndContainsValue() {
        CsvReaderRowResult<String> result = CsvReaderRowResult.success("hello");

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.parsedRow()).isPresent().contains("hello");
        assertThat(result.exception()).isEmpty();
    }

    @Test
    void isNotSuccessAndContainsException() {
        ExceptionRecord ex = new ExceptionRecord("ID1", "trades.csv", "PARSE_ERROR", "bad row", Map.of());
        CsvReaderRowResult<String> result = CsvReaderRowResult.failure(ex);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.parsedRow()).isEmpty();
        assertThat(result.exception()).isPresent();
        assertThat(result.exception().get().exceptionType()).isEqualTo("PARSE_ERROR");
    }

    @Test
    void storesRowNumberAndData() {
        Map<String, String> data = Map.of("id", "1", "name", "foo");
        CsvRow row = new CsvRow(5L, data);

        assertThat(row.rowNumber()).isEqualTo(5L);
        assertThat(row.data()).containsEntry("id", "1").containsEntry("name", "foo");
    }

    private Path writeCsv(String content) throws IOException {
        Path file = tempDir.resolve("test.csv");
        Files.writeString(file, content);
        return file;
    }
}