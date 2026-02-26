package com.onechronos.darkpool.etl.extract;

import java.util.Map;

/**
 * Row to encapsulate row number and map containing row contents.
 */
public record CsvRow(Long rowNumber, Map<String, String> data) {}
