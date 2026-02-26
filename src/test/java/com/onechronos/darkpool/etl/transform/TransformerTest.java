package com.onechronos.darkpool.etl.transform;

import com.onechronos.darkpool.etl.config.ValidationConfig;
import com.onechronos.darkpool.etl.model.*;
import com.onechronos.darkpool.etl.model.enums.Sector;
import com.onechronos.darkpool.etl.model.enums.TradeStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TransformerTest {

    private static final Path SOURCE_FILE = Path.of("trades.csv");
    private static final Instant TRADE_TIME = Instant.parse("2024-01-15T10:00:00Z");
    private static final Instant FILL_TIME  = Instant.parse("2024-01-15T11:00:00Z");

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        ValidationConfig validationConfig = new ValidationConfig(new BigDecimal("0.01"));

        Map<String, SymbolRefRecord> symbolMap = Map.of(
                "AAPL", new SymbolRefRecord("AAPL", "Apple Inc.", Sector.TECHNOLOGY, true),
                "MSFT", new SymbolRefRecord("MSFT", "Microsoft Corp.", Sector.TECHNOLOGY, false)
        );

        Map<String, FillRecord> fillMap = Map.of(
                "TRD001", new FillRecord("EXT001", "TRD001", FILL_TIME, "AAPL", 100, new BigDecimal("150.00"), "CP1"),
                "TRD003", new FillRecord("EXT003", "TRD003", FILL_TIME, "AAPL", 999, new BigDecimal("999.99"), "CP1")
        );

        transformer = Transformer.build(validationConfig, symbolMap, fillMap);
    }

    @Test
    void producesCleanedTradeWhenAllValid() {
        TradeRecord trade = trade("TRD001", "AAPL", 100, "150.00");

        TransformerResult result = transformer.transform(trade, SOURCE_FILE);

        assertThat(result.cleanedTrade()).isPresent();
        CleanedTradeRecord cleaned = result.cleanedTrade().get();
        assertThat(cleaned.tradeId()).isEqualTo("TRD001");
        assertThat(cleaned.counterpartyConfirmed()).isTrue();
        assertThat(cleaned.discrepancyFlag()).isFalse();
    }

    @Test
    void rejectsDuplicateTradeId() {
        TradeRecord trade = trade("TRD001", "AAPL", 100, "150.00");

        transformer.transform(trade, SOURCE_FILE);
        TransformerResult duplicate = transformer.transform(trade, SOURCE_FILE);

        assertThat(duplicate.cleanedTrade()).isEmpty();
        assertThat(duplicate.exception().get().exceptionType()).isEqualTo("DUPLICATE_TRADE_ID");
    }

    @Test
    void rejectsInvalidSymbol() {
        TradeRecord trade = trade("TRD002", "INVALID", 100, "150.00");

        TransformerResult result = transformer.transform(trade, SOURCE_FILE);

        assertThat(result.exception().get().exceptionType()).isEqualTo("INVALID_SYMBOL");
    }

    @Test
    void rejectsInactiveSymbol() {
        TradeRecord trade = trade("TRD002", "MSFT", 100, "150.00");

        TransformerResult result = transformer.transform(trade, SOURCE_FILE);

        assertThat(result.exception().get().exceptionType()).isEqualTo("INACTIVE_SYMBOL");
    }

    @Test
    void setDiscrepancyFlagWhenPriceOrQuantityMismatch() {
        TradeRecord trade = trade("TRD003", "AAPL", 100, "150.00");

        TransformerResult result = transformer.transform(trade, SOURCE_FILE);

        assertThat(result.cleanedTrade()).isPresent();
        assertThat(result.cleanedTrade().get().discrepancyFlag()).isTrue();
    }

    @Test
    void setsCounterpartyConfirmedFalseWhenNoFillFound() {
        TradeRecord trade = trade("TRD_NO_FILL", "AAPL", 100, "150.00");

        TransformerResult result = transformer.transform(trade, SOURCE_FILE);

        assertThat(result.cleanedTrade()).isPresent();
        assertThat(result.cleanedTrade().get().counterpartyConfirmed()).isFalse();
        assertThat(result.cleanedTrade().get().discrepancyFlag()).isFalse();
    }

    private TradeRecord trade(String tradeId, String symbol, int quantity, String price) {
        return new TradeRecord(
                tradeId, TRADE_TIME, symbol, quantity,
                new BigDecimal(price), "BUY1", "SEL1",
                TradeStatus.EXECUTED, Map.of()
        );
    }
}