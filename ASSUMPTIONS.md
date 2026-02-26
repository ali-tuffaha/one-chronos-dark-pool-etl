### List of assumptions I made:
- CSV: Header is always at the first row.
- CSV: We should skip empty lines when reading csv file.
- Parsing: if trade id is missing then replace it with UNKNOWN in exception report.
- Transforming: if counterpartyConfirmed is not confirmed then discrepancyFlag is false.
- Transforming: no deduplication needed in Fills file.
