# OneChronos Dark Pool ETL

## Requirements

- Java 17
- Maven 3.9.9

## Build

```bash
mvn clean package
```

## Run

```bash
# With a config file
java -jar target/one-chronos-dark-pool-etl-1.0-SNAPSHOT.jar -c /path/to/config.conf

# Without a config file (loads default application.conf under src/main/resources/application.conf)
java -jar target/one-chronos-dark-pool-etl-1.0-SNAPSHOT.jar
```

## Config File

```hocon
# Paths to input CSV files
read-config {
  symbols-ref-file = "data/symbols_reference.csv"  # valid symbols reference input file path
  fills-file       = "data/counterparty_fills.csv"  # trade confirmations from counterparties input file path
  trades-file      = "data/trades.csv"              # raw trade executions input file path
}

# Paths to output JSON files (directories created automatically if they do not exist)
write-config {
  cleaned-trades-file    = "output/cleaned_trades.json"      # validated and cleaned trades ouput file path 
  exceptions-report-file = "output/exceptions_report.json"   # trade exception report output file path
}

# Validation thresholds
validation-config {
  price-discrepancy-threshold = 0.01  # maximum allowed price difference between trade and fill
}
```

## Run Tests

```bash
mvn test
```