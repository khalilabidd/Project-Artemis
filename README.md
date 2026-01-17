# Project Artemis - Parquet File Comparator

A Python utility for comparing Parquet files to detect schema changes, value differences, and potential data regressions.

## Overview

This project provides a `ParquetComparator` class that compares two Parquet files and generates comprehensive reports on:
- **Schema Changes**: Detects added/removed columns and type changes
- **Value Changes**: Identifies rows with modified values and row count differences
- **Regression Detection**: Flags potential data quality issues

## Features

✨ **Schema Comparison**
- Detects newly added columns
- Identifies removed columns
- Reports data type changes for common columns

📊 **Value Analysis**
- Compares row counts between versions
- Identifies which columns have value changes
- Calculates percentage of affected rows

⚠️ **Regression Detection**
- Automatically detects potential regressions
- Flags breaking changes (removed columns, type changes)
- Alerts on data loss scenarios

📈 **Formatted Reporting**
- Human-readable console output
- JSON export capability
- Structured statistics for programmatic use

## Requirements

- Python 3.7+
- pandas
- pyarrow

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Project-Artemis
```

2. Install dependencies:
```bash
pip install pandas pyarrow
```

## Usage

### Basic Usage

```python
from compare_parquet import ParquetComparator

# Initialize comparator with current and previous parquet files
comparator = ParquetComparator(
    current_file="data_current.parquet",
    previous_file="data_previous.parquet"
)

# Generate and print report
stats = comparator.print_report()

# Save report to JSON
import json
with open("comparison_report.json", "w") as f:
    json.dump(stats, f, indent=2)
```

### Available Methods

- **`compare_schema()`**: Returns schema differences (added/removed columns, type changes)
- **`compare_values()`**: Returns value differences (row count changes, columns with modifications)
- **`generate_statistics()`**: Comprehensive statistics including regression detection
- **`print_report()`**: Prints formatted report and returns statistics

## Output Example

```
============================================================
PARQUET FILE COMPARISON REPORT
============================================================

📊 SCHEMA CHANGES:
  Added columns: ['new_column']
  Removed columns: []
  Type changes: {'age': {'previous': 'int64', 'current': 'int32'}}

📈 VALUE CHANGES:
  Previous rows: 1000
  Current rows: 1050
  Row difference: 50

  Columns with value changes:
    - status: 15 rows (1.50%)

✅ NO REGRESSION DETECTED
  Potential regression: False
============================================================
```

## Use Cases

- **Data Pipeline Monitoring**: Detect unexpected changes in production data
- **Version Control**: Compare data versions before/after ETL processes
- **Quality Assurance**: Identify data anomalies and regressions
- **Schema Evolution**: Track schema changes over time

