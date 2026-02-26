package com.onechronos.darkpool.etl.extract;

import com.onechronos.darkpool.etl.model.FillRecord;
import com.onechronos.darkpool.etl.model.SymbolRefRecord;
import com.onechronos.darkpool.etl.model.TradeRecord;
import com.onechronos.darkpool.etl.model.enums.Sector;
import com.onechronos.darkpool.etl.model.enums.TradeStatus;
import com.onechronos.darkpool.etl.model.ExceptionRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class CsvMappers {
    private CsvMappers() {
    }

    /**
     * Holds the result of parsing a single field — either a value or an error message.
     */
    private record Field<T>(T value, String error) {
        static <T> Field<T> of(T value) {
            return new Field<>(value, null);
        }

        static <T> Field<T> error(String msg) {
            return new Field<>(null, msg);
        }

        boolean isError() {
            return Objects.nonNull(error);
        }
    }

    /**
     * Function that maps a csv row map to a CsvReaderRowResult
     *
     * @param row input row with map
     *            * @param sourceFile of source file for reporting purposes
     * @return CsvReaderRowResult which contains either parsed Trade record or an ExceptionRecord
     */
    public static CsvReaderRowResult<TradeRecord> toTradeRecord(CsvRow row, Path sourceFile) {
        Field<String> tradeId = requiredStringField(row.data(), "trade_id");
        Field<String> symbol = requiredStringFieldUpperCase(row.data(), "symbol");
        Field<String> buyerId = requiredStringField(row.data(), "buyer_id");
        Field<String> sellerId = requiredStringField(row.data(), "seller_id");
        Field<Instant> timestamp = requiredTimestampField(row.data(), "timestamp");
        Field<BigDecimal> price = requiredPriceField(row.data(), "price");
        Field<Integer> quantity = requiredIntegerField(row.data(), "quantity");
        Field<TradeStatus> tradeStatus = requiredTradeStatusField(row.data(), "trade_status");

        List<String> parseErrors = errors(tradeId, symbol, buyerId, sellerId, timestamp, price, quantity, tradeStatus);
        if (!parseErrors.isEmpty()) {
            String recordId = tradeId.isError() ? "UNKNOWN" : tradeId.value();
            return CsvReaderRowResult.failure(parseError(recordId, sourceFile.toString(), parseErrors, row));
        }

        return CsvReaderRowResult.success(new TradeRecord(
                tradeId.value(),
                timestamp.value(),
                symbol.value(),
                quantity.value(),
                price.value(),
                buyerId.value(),
                sellerId.value(),
                tradeStatus.value(),
                row.data()
        ));
    }

    /**
     * Function that maps a csv row map to a CsvReaderRowResult
     *
     * @param row        input row with map
     * @param sourceFile of source file for reporting purposes
     * @return CsvReaderRowResult which contains either parsed Fill record or an ExceptionRecord
     */
    public static CsvReaderRowResult<FillRecord> toFillRecord(CsvRow row, Path sourceFile) {
        Field<String> externalRefId = requiredStringField(row.data(), "external_ref_id");
        Field<String> ourTradeId = requiredStringField(row.data(), "our_trade_id");
        Field<String> symbol = requiredStringFieldUpperCase(row.data(), "symbol");
        Field<String> counterpartyId = requiredStringField(row.data(), "counterparty_id");
        Field<Instant> timestamp = requiredTimestampField(row.data(), "timestamp");
        Field<BigDecimal> price = requiredPriceField(row.data(), "price");
        Field<Integer> quantity = requiredIntegerField(row.data(), "quantity");

        List<String> parseErrors = errors(externalRefId, ourTradeId, symbol, counterpartyId, timestamp, price, quantity);
        if (!parseErrors.isEmpty()) {
            String recordId = externalRefId.isError() ? "UNKNOWN" : externalRefId.value();
            return CsvReaderRowResult.failure(parseError(recordId, sourceFile.toString(), parseErrors, row));
        }

        return CsvReaderRowResult.success(new FillRecord(
                externalRefId.value(),
                ourTradeId.value(),
                timestamp.value(),
                symbol.value(),
                quantity.value(),
                price.value(),
                counterpartyId.value()
        ));
    }

    /**
     * Function that maps a csv row map to a CsvReaderRowResult
     *
     * @param row        input row with map
     * @param sourceFile of source file for reporting purposes
     * @return CsvReaderRowResult which contains either parsed SymbolRef record or an ExceptionRecord
     */
    public static CsvReaderRowResult<SymbolRefRecord> toSymbolRefRecord(CsvRow row, Path sourceFile) {
        Field<String> symbol = requiredStringFieldUpperCase(row.data(), "symbol");
        Field<String> companyName = requiredStringField(row.data(), "company_name");
        Field<String> isActiveRaw = requiredStringField(row.data(), "is_active");
        Field<Sector> sector = requiredSectorField(row.data(), "sector");

        List<String> parseErrors = errors(symbol, companyName, isActiveRaw, sector);
        if (!parseErrors.isEmpty()) {
            String recordId = symbol.isError() ? "UNKNOWN" : symbol.value();
            return CsvReaderRowResult.failure(parseError(recordId, sourceFile.toString(), parseErrors, row));
        }

        return CsvReaderRowResult.success(new SymbolRefRecord(
                symbol.value(),
                companyName.value(),
                sector.value(),
                Boolean.parseBoolean(isActiveRaw.value().trim())
        ));
    }

    // -------------------------------------------------------------------------
    // Field parsers
    // -------------------------------------------------------------------------

    private static Field<String> requiredStringFieldUpperCase(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : Field.of(value.trim().toUpperCase());
    }

    private static Field<String> requiredStringField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : Field.of(value.trim());
    }

    private static Field<Instant> requiredTimestampField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : parseTimestamp(value, fieldName);
    }

    private static Field<Instant> parseTimestamp(String raw, String fieldName) {
        for (TimestampParser parser : TimestampParser.values()) {
            try {
                return Field.of(parser.parse(raw));
            } catch (Exception ignored) {
            }
        }
        return Field.error("Field %s contains unparsable timestamp: %s".formatted(fieldName, raw));
    }

    private static Field<BigDecimal> requiredPriceField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : parsePrice(value, fieldName);
    }

    private static Field<BigDecimal> parsePrice(String raw, String fieldName) {
        try {
            BigDecimal price = new BigDecimal(raw.trim()).setScale(2, RoundingMode.HALF_UP);
            return price.compareTo(BigDecimal.ZERO) <= 0
                    ? Field.error("Price must be positive: " + raw)
                    : Field.of(price);
        } catch (NumberFormatException e) {
            return Field.error("Field %s contains unparsable price: %s".formatted(fieldName, raw));
        }
    }

    private static Field<Integer> requiredIntegerField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : parseInteger(value, fieldName);
    }

    private static Field<Integer> parseInteger(String raw, String fieldName) {
        try {
            int quantity = Integer.parseInt(raw.trim());
            return quantity <= 0
                    ? Field.error("Integer must be positive: " + raw)
                    : Field.of(quantity);
        } catch (NumberFormatException e) {
            return Field.error("Field %s contains unparsable integer: %s".formatted(fieldName, raw));
        }
    }

    private static Field<TradeStatus> requiredTradeStatusField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : parseTradeStatus(value, fieldName);
    }

    private static Field<TradeStatus> parseTradeStatus(String raw, String fieldName) {
        try {
            return Field.of(TradeStatus.parse(raw));
        } catch (IllegalArgumentException e) {
            return Field.error("Field %s contains unparsable trade status: %s".formatted(fieldName, raw));
        }
    }

    private static Field<Sector> requiredSectorField(Map<String, String> row, String fieldName) {
        String value = row.get(fieldName);
        return (Objects.isNull(value) || value.isBlank())
                ? Field.error("Missing required field: " + fieldName)
                : parseSector(value, fieldName);
    }

    private static Field<Sector> parseSector(String raw, String fieldName) {
        try {
            return Field.of(Sector.parse(raw));
        } catch (IllegalArgumentException e) {
            return Field.error("Field %s contains unparsable sector: %s".formatted(fieldName, raw));
        }
    }

    /**
     * Timestamp formats: first match wins
     */
    private enum TimestampParser {
        EPOCH {
            @Override
            Instant parse(String raw) {
                return Instant.ofEpochSecond(Long.parseLong(raw.trim()));
            }
        },
        ISO_8601 {
            @Override
            Instant parse(String raw) {
                return Instant.parse(raw.trim());
            }
        },
        US_FORMAT {
            @Override
            Instant parse(String raw) {
                return LocalDateTime
                        .parse(raw.trim(), DateTimeFormatter.ofPattern("M/d/yyyy H:m:s"))
                        .toInstant(ZoneOffset.UTC);
            }
        };

        abstract Instant parse(String raw) throws Exception;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<String> errors(Field<?>... fields) {
        return Stream.of(fields)
                .filter(Field::isError)
                .map(Field::error)
                .toList();
    }

    private static ExceptionRecord parseError(
            String recordId,
            String sourceFile,
            List<String> parseErrors,
            CsvRow csvRow
    ) {
        String detailsWithRow = "Row %d: %s".formatted(csvRow.rowNumber(), String.join("; ", parseErrors));
        return new ExceptionRecord(recordId, sourceFile, "PARSE_ERROR", detailsWithRow, csvRow.data());
    }

}