### List of assumptions I made:
- CSV: Header is always at the first row.
- CSV: We should skip empty lines when reading csv file.
- CSV: symbols and fills files are small enough to fit in memory.
- Parsing: if trade id is missing then replace it with UNKNOWN in exception report.
- Transforming: if counterpartyConfirmed is not confirmed then discrepancyFlag is false.
- Transforming: no deduplication needed in Fills file.
