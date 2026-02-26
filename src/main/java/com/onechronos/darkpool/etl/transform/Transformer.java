package com.onechronos.darkpool.etl.transform;

import com.onechronos.darkpool.etl.config.ValidationConfig;
import com.onechronos.darkpool.etl.exception.TransformerException;
import com.onechronos.darkpool.etl.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.*;

/**
 * Class that encapsulates validations and transformations applied to TradeRecord to produce CleanedTradeRecord
 * Trade Record:
 * - Deduplicate on trade_id field (include in exception report if duplicates found)
 * - Validate symbol in trade record exists in symbol references (include in exception report if invalid symbol)
 * - Validate symbol in trade record is active (include in exception report if inactive symbol)
 * - Validate a fill record exists for the trade record  (include in exception report if no fill matched with trade record)
 * - Validate discrepancies in quantity and price between trade and corresponding fill record (if discrepancy found then set flags as true)
 * - Validate trade record and fill record have matching symbol (include in exception report if not matching symbols)
 * - Validate fill record timestamp occurred after trade record timestamp (include in exception report trade occurred after fill)
 */
public class Transformer {
    private static final Logger log = LoggerFactory.getLogger(Transformer.class);

    private final ValidationConfig validationConfig;
    private final Map<String, SymbolRefRecord> symbolMap;
    private final Map<String, FillRecord> fillMap;
    private final Set<String> seenTradeIds = new HashSet<>();

    private Transformer(
            ValidationConfig validationConfig,
            Map<String, SymbolRefRecord> symbolMap,
            Map<String, FillRecord> fillMap
    ) {
        this.validationConfig = validationConfig;
        this.symbolMap = symbolMap;
        this.fillMap = fillMap;
    }

    public static Transformer build(
            ValidationConfig validationConfig,
            Map<String, SymbolRefRecord> symbolMap,
            Map<String, FillRecord> fillMap
    ) {
        return new Transformer(validationConfig, symbolMap, fillMap);
    }

    /**
     * Function that maps TradeRecord to CleanedTradeRecord
     * validations applied:
     * - Deduplicate on trade_id field (include in exception report if duplicates found)
     * - Validate symbol in trade record exists in symbol references (include in exception report if invalid symbol)
     * - Validate symbol in trade record is active (include in exception report if inactive symbol)
     * - Validate a fill record exists for the trade record  (include in exception report if no fill matched with trade record)
     * - Validate discrepancies in quantity and price between trade and corresponding fill record (if discrepancy found then set flags as true)
     * - Validate trade record and fill record have matching symbol (include in exception report if not matching symbols)
     * - Validate fill record timestamp occurred after trade record timestamp (include in exception report trade occurred after fill)
     *
     * @param trade      input
     * @param sourceFile path of source file for reporting
     * @return TransformerResult either containing CleanedTradeRecord or ExceptionRecord
     * @throws TransformerException if an unexpected exception occurs
     */
    public TransformerResult transform(TradeRecord trade, Path sourceFile) throws TransformerException {
        try {
            // Check for duplicates
            if (!seenTradeIds.add(trade.tradeId())) {
                log.debug("Duplicate trade_id: {}", trade.tradeId());
                return TransformerResult.rejected(new ExceptionRecord(
                        trade.tradeId(),
                        sourceFile.toString(),
                        "DUPLICATE_TRADE_ID",
                        "Duplicate trade_id: " + trade.tradeId(),
                        trade.rawData()
                ));
            }

            // Check for Symbol validity
            SymbolRefRecord tradeSymbolRef = symbolMap.get(trade.symbol());
            if (Objects.isNull(tradeSymbolRef)) {
                return TransformerResult.rejected(new ExceptionRecord(
                        trade.tradeId(),
                        sourceFile.toString(),
                        "INVALID_SYMBOL",
                        "Symbol in trade record not found in reference data: " + trade.symbol(),
                        trade.rawData()
                ));
            }
            if (!tradeSymbolRef.isActive()) {
                return TransformerResult.rejected(new ExceptionRecord(
                        trade.tradeId(),
                        sourceFile.toString(),
                        "INACTIVE_SYMBOL",
                        "Symbol in trade record is inactive: " + trade.symbol(),
                        trade.rawData()
                ));
            }

            FillRecord fill = fillMap.get(trade.tradeId());

            // Check for discrepancy validity
            boolean counterpartyConfirmed = Objects.nonNull(fill);
            boolean discrepancyFlag = counterpartyConfirmed && hasDiscrepancy(trade, fill);

            // Validate fill symbol matches trade symbol
            if (counterpartyConfirmed && !fill.symbol().equals(trade.symbol())) {
                return TransformerResult.rejected(new ExceptionRecord(
                        trade.tradeId(),
                        sourceFile.toString(),
                        "FILL_SYMBOL_MISMATCH",
                        "Fill symbol %s does not match trade symbol %s".formatted(fill.symbol(), trade.symbol()),
                        trade.rawData()
                ));
            }

            // Validate fill timestamp is after trade timestamp
            if (counterpartyConfirmed && !fill.timestamp().isAfter(trade.timestamp())) {
                return TransformerResult.rejected(new ExceptionRecord(
                        trade.tradeId(),
                        sourceFile.toString(),
                        "FILL_TIMESTAMP_INVALID",
                        "Fill timestamp %s is not after trade timestamp %s".formatted(fill.timestamp(), trade.timestamp()),
                        trade.rawData()
                ));
            }

            return TransformerResult.clean(new CleanedTradeRecord(
                    trade.tradeId(),
                    trade.timestamp(),
                    trade.symbol(),
                    trade.quantity(),
                    trade.price(),
                    trade.buyerId(),
                    trade.sellerId(),
                    counterpartyConfirmed,
                    discrepancyFlag
            ));
        } catch (Exception e) {
            throw new TransformerException("Fatal Error while transforming Trade Record", e);
        }
    }

    /**
     * Validate discrepancies in quantity and price between trade and corresponding fill record
     */
    private boolean hasDiscrepancy(TradeRecord trade, FillRecord fill) {
        BigDecimal priceDiff = trade.price().subtract(fill.price()).abs();

        boolean priceDiscrepancy = priceDiff.compareTo(validationConfig.priceDiscrepancyThreshold()) > 0;

        boolean quantityDiscrepancy = !trade.quantity().equals(fill.quantity());

        return priceDiscrepancy || quantityDiscrepancy;
    }
}
