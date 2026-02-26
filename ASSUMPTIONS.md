## List of assumptions:
### CSV Files
- Header is always at the first row.
- We should skip empty lines when reading csv file.
- Symbols and fills files are small enough to fit in memory.
- Trades are streamed rather than loaded into memory to support large file sizes.
- Parse failures in symbols and fills files are logged and skipped. Not written to exceptions report.

### Parsing:
- if trade id is missing then replace it with UNKNOWN in exception report.
- Timestamps are tried in order: Unix epoch, ISO 8601, M/d/yyyy H:m:s (UTC) (first match wins).
- Prices are rounded to 2 decimal places (HALF_UP).
- Prices and quantities must be positive. Zero or negative is a parse error.
- Symbols are normalised to uppercase at parse time.

### Transforming:
- If counterpartyConfirmed is not confirmed then discrepancyFlag is false.
- No deduplication needed in Fills file.
- First occurrence of a duplicate trade_id is kept, all subsequent are rejected.
- If trade record is cancelled then no need to check if it matches with a fill record for it and just filter silently.
- Fill symbol mismatch and fill timestamp invalid are rejections, not flags. 
- If no fill exists for a trade, counterparty_confirmed: false and discrepancy_flag: false

### Output:
- Cancelled trades are not written to either output file.