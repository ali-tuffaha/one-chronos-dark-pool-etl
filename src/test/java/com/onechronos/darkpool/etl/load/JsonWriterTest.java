package com.onechronos.darkpool.etl.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onechronos.darkpool.etl.model.CleanedTradeRecord;
import com.onechronos.darkpool.etl.model.ExceptionRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JsonWriterTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void writesCleanedTradeAsSnakeCaseJson() throws Exception {
        Path cleanedFile = tempDir.resolve("cleaned_trades.json");
        Path exceptionsFile = tempDir.resolve("exceptions_report.json");

        CleanedTradeRecord trade = new CleanedTradeRecord(
                "TRD001",
                Instant.parse("2024-01-15T10:00:00Z"),
                "AAPL",
                100,
                new BigDecimal("150.00"),
                "BUY1",
                "SEL1",
                true,
                false
        );

        try (JsonWriter writer = JsonWriter.open(cleanedFile, exceptionsFile)) {
            writer.writeCleanedTrade(trade);
        }

        JsonNode root = mapper.readTree(cleanedFile.toFile());
        JsonNode record = root.get(0);

        assertThat(record.get("trade_id").asText()).isEqualTo("TRD001");
        assertThat(record.get("timestamp_utc").asText()).isEqualTo("2024-01-15T10:00:00Z");
        assertThat(record.get("symbol").asText()).isEqualTo("AAPL");
        assertThat(record.get("counterparty_confirmed").asBoolean()).isTrue();
        assertThat(record.get("discrepancy_flag").asBoolean()).isFalse();
    }

    @Test
    void writesExceptionRecordAsSnakeCaseJson() throws Exception {
        Path cleanedFile = tempDir.resolve("cleaned_trades.json");
        Path exceptionsFile = tempDir.resolve("exceptions_report.json");

        ExceptionRecord exception = new ExceptionRecord(
                "TRD002",
                "trades.csv",
                "PARSE_ERROR",
                "Row 5: Missing required field: price",
                Map.of("trade_id", "TRD002")
        );

        try (JsonWriter writer = JsonWriter.open(cleanedFile, exceptionsFile)) {
            writer.writeException(exception);
        }

        JsonNode root = mapper.readTree(exceptionsFile.toFile());
        JsonNode record = root.get(0);

        assertThat(record.get("record_id").asText()).isEqualTo("TRD002");
        assertThat(record.get("source_file").asText()).isEqualTo("trades.csv");
        assertThat(record.get("exception_type").asText()).isEqualTo("PARSE_ERROR");
        assertThat(record.get("details").asText()).contains("price");
    }
}