package com.onechronos.darkpool.etl.extract;

import com.onechronos.darkpool.etl.model.FillRecord;
import com.onechronos.darkpool.etl.model.SymbolRefRecord;
import com.onechronos.darkpool.etl.model.TradeRecord;
import com.onechronos.darkpool.etl.model.enums.Sector;
import com.onechronos.darkpool.etl.model.enums.TradeStatus;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CsvMappersTest {

    @Test
    void mapValidTradeRowSuccessfully() {
        CsvRow row = row(2L, Map.of(
                "trade_id", "TRD001",
                "symbol", "aapl",
                "buyer_id", "BUY1",
                "seller_id", "SEL1",
                "timestamp", "2024-01-15T10:00:00Z",
                "price", "150.00",
                "quantity", "100",
                "trade_status", "EXECUTED"
        ));

        CsvReaderRowResult<TradeRecord> result = CsvMappers.toTradeRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isTrue();
        TradeRecord trade = result.parsedRow().get();
        assertThat(trade.tradeId()).isEqualTo("TRD001");
        assertThat(trade.symbol()).isEqualTo("AAPL");
        assertThat(trade.price()).isEqualByComparingTo("150.00");
        assertThat(trade.tradeStatus()).isEqualTo(TradeStatus.EXECUTED);
    }

    @Test
    void mapTradeRecordWithMissingRequiredField() {
        CsvRow row = row(3L, Map.of(
                "trade_id", "TRD002",
                "symbol", "AAPL",
                "buyer_id", "BUY1",
                "seller_id", "SEL1",
                "timestamp", "2024-01-15T10:00:00Z",
                "quantity", "100",
                "trade_status", "EXECUTED"
                // price is missing
        ));

        CsvReaderRowResult<TradeRecord> result = CsvMappers.toTradeRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.exception()).isPresent();
        assertThat(result.exception().get().exceptionType()).isEqualTo("PARSE_ERROR");
        assertThat(result.exception().get().sourceFile()).isEqualTo("test.csv");
        assertThat(result.exception().get().details()).contains("price");
    }

    @Test
    void mapValidFillRowSuccessfully() {
        CsvRow row = row(2L, Map.of(
                "external_ref_id", "EXT001",
                "our_trade_id", "TRD001",
                "symbol", "msft",
                "counterparty_id", "CP1",
                "timestamp", "2024-01-15T11:00:00Z",
                "price", "299.99",
                "quantity", "50"
        ));

        CsvReaderRowResult<FillRecord> result = CsvMappers.toFillRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isTrue();
        FillRecord fill = result.parsedRow().get();
        assertThat(fill.externalRefId()).isEqualTo("EXT001");
        assertThat(fill.ourTradeId()).isEqualTo("TRD001");
        assertThat(fill.symbol()).isEqualTo("MSFT");
        assertThat(fill.price()).isEqualByComparingTo("299.99");
    }

    @Test
    void mapFillRecordWithMissingRequiredField() {
        CsvRow row = row(4L, Map.of(
                "external_ref_id", "EXT002",
                "our_trade_id", "TRD002",
                "symbol", "MSFT",
                "counterparty_id", "CP1",
                "timestamp", "2024-01-15T11:00:00Z",
                "price", "299.99"
                // quantity is missing
        ));

        CsvReaderRowResult<FillRecord> result = CsvMappers.toFillRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.exception().get().sourceFile()).isEqualTo("test.csv");
        assertThat(result.exception().get().details()).contains("quantity");
    }

    @Test
    void mapSymbolRefRowSuccessfully() {
        CsvRow row = row(2L, Map.of(
                "symbol", "googl",
                "company_name", "Alphabet Inc.",
                "is_active", "true",
                "sector", "Technology"
        ));

        CsvReaderRowResult<SymbolRefRecord> result = CsvMappers.toSymbolRefRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isTrue();
        SymbolRefRecord symbol = result.parsedRow().get();
        assertThat(symbol.symbol()).isEqualTo("GOOGL");
        assertThat(symbol.isActive()).isTrue();
        assertThat(symbol.sector()).isEqualTo(Sector.TECHNOLOGY);
    }

    @Test
    void mapSymbolRefRecordWithMissingRequiredField() {
        CsvRow row = row(5L, Map.of(
                "symbol", "XYZ",
                "company_name", "XYZ Corp",
                "is_active", "true",
                "sector", "UNKNOWN_SECTOR"
        ));

        CsvReaderRowResult<SymbolRefRecord> result = CsvMappers.toSymbolRefRecord(row, Path.of("test.csv"));

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.exception().get().sourceFile()).isEqualTo("test.csv");
        assertThat(result.exception().get().details()).contains("sector");
    }


    @Test
    void parsesEpochTimestamp() {
        CsvRow row = row(2L, Map.of(
                "trade_id", "TRD003",
                "symbol", "AAPL",
                "buyer_id", "BUY1",
                "seller_id", "SEL1",
                "timestamp", "1705312800",
                "price", "150.00",
                "quantity", "100",
                "trade_status", "EXECUTED"
        ));

        assertThat(CsvMappers.toTradeRecord(row, Path.of("test.csv")).isSuccess()).isTrue();
    }

    @Test
    void parseUsFormatTimestamp() {
        CsvRow row = row(2L, Map.of(
                "trade_id", "TRD004",
                "symbol", "AAPL",
                "buyer_id", "BUY1",
                "seller_id", "SEL1",
                "timestamp", "1/15/2024 10:5:7",
                "price", "150.00",
                "quantity", "100",
                "trade_status", "EXECUTED"
        ));

        assertThat(CsvMappers.toTradeRecord(row, Path.of("test.csv")).isSuccess()).isTrue();
    }

    private CsvRow row(long rowNumber, Map<String, String> data) {
        return new CsvRow(rowNumber, data);
    }
}