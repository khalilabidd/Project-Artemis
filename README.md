# Project Artemis — Parquet/DataFrame Comparator

Lightweight Python utility to compare two datasets (Pandas DataFrames or Parquet files) and surface schema changes, value differences, and potential regressions.

**Key points**
- Primary comparator: `DFComparator` implemented in `compare_data.py`.
- Includes a plain-Python test runner at `tests/run_tests.py` and pytest-compatible tests in `tests/test_compare_parquet.py`.

## Features

- Schema diff: added / removed columns and dtype changes
- Value diff: row count changes and per-column changed-row counts
- Null-aware comparisons and support for duplicate primary-key values via a merge fallback
- Human-readable console report via `print_report()` and programmatic `generate_statistics()` output

## Requirements

- Python 3.8+
- pandas
- (optional) pyarrow for Parquet read/write
- (optional) pytest for running tests with the pytest runner

Install minimal dependencies:

```bash
pip install pandas
# install pyarrow if you need parquet IO
pip install pyarrow
```

Install pytest (optional):

```bash
pip install pytest
```

## Usage

Compare two Parquet files (read them with pandas) or compare DataFrames directly.

Example (Parquet files):

```python
import pandas as pd
from compare_data import DFComparator

df_prev = pd.read_parquet('data_previous.parquet')
df_curr = pd.read_parquet('data_current.parquet')

comp = DFComparator(df_curr, df_prev, primary_keys=['id'])
stats = comp.print_report()
```

Example (construct DataFrames in code):

```python
import pandas as pd
from compare_data import DFComparator

prev = pd.DataFrame({'id':[1,2], 'val':[10,20]})
curr = pd.DataFrame({'id':[1,2], 'val':[11,20], 'status':['active','active']})

comp = DFComparator(curr, prev, primary_keys=['id'])
stats = comp.generate_statistics()
print(stats)
```

## Running tests

Quick (no external test runner):

```bash
python tests/run_tests.py
```

With pytest (preferred for integrations):

```bash
pip install pytest
python -m pytest -q
```

## Files of interest

- `compare_data.py` — main comparator (`DFComparator` class)
- `tests/run_tests.py` — plain-Python test runner for rapid checks
- `tests/test_compare_parquet.py` — pytest-style tests (can be run with `pytest`)

## Notes & Recommendations

- `DFComparator` accepts DataFrames; reading Parquet is left to the caller so you can choose the engine (`pyarrow` or `fastparquet`).
- Primary keys are optional; when provided the comparator will attempt index-based comparisons and fall back to a merge-based strategy for duplicate keys.
- The comparator uses null-aware equality (NaN/None considered equal to NaN/None) and attempts to avoid false positives from dtype-only differences.

