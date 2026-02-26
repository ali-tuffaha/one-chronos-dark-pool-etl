package com.onechronos.darkpool.etl.transform;

import com.onechronos.darkpool.etl.model.CleanedTradeRecord;
import com.onechronos.darkpool.etl.model.ExceptionRecord;

import java.util.Optional;

/**
 * The outcome of transforming a single TradeRecord.
 */
public record TransformerResult(
        Optional<CleanedTradeRecord> cleanedTrade,
        Optional<ExceptionRecord> exception
) {

    public boolean isClean() {
        return cleanedTrade.isPresent();
    }

    public static TransformerResult clean(CleanedTradeRecord trade) {
        return new TransformerResult(Optional.of(trade), Optional.empty());
    }

    public static TransformerResult rejected(ExceptionRecord exception) {
        return new TransformerResult(Optional.empty(), Optional.of(exception));
    }
}