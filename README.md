# YAML-driven Synthetic Data Generator

Python utility that generates synthetic tabular datasets driven by a YAML configuration. It supports referential integrity between tables (foreign keys) and can output data as CSV, JSON, DuckDB, or Parquet.

## Features

- YAML configuration for tables and fields
- Faker-based values for common personal / date / text data
- Sequence generators for primary keys
- Weighted random choices (`weighted_choice`)
- Referential integrity via `fk: other_table.field`
- Config validation with helpful error messages for invalid FK references
- Dry-run mode to validate and preview planned table generation
- Progress bar with per-table and overall elapsed time
- Output formats: CSV, JSON, DuckDB, Parquet (single file or chunked multi-file)

## Project structure

```
datafaker/
├── src/
│   ├── fakercore/                 # shared base (not published separately)
│   │   ├── base.py                #   BaseDataGenerator — template hooks, output writers, tqdm
│   │   └── cli_base.py            #   shared argument parser + dispatch
│   ├── datafaker/                 # pip install datafaker
│   │   ├── generator.py           #   DataGenerator(BaseDataGenerator)
│   │   └── cli.py                 #   entry point → datafaker
│   └── scenariofaker/             # pip install scenariofaker
│       ├── generator.py           #   ScenarioDataGenerator — scenario & time-profile logic
│       ├── _scenarios.py          #   pure helpers: null bursts, outliers, duplicates …
│       ├── _time_profile.py       #   pure helpers: weighted timestamps
│       └── cli.py                 #   entry point → scenariofaker
├── examples/
├── tests/
├── scripts/
├── pyproject.toml
└── requirements.txt
```

## Installation

```cmd
cd C:\Users\Patrick\code\portfolio\datafaker
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e .
```

For chunked Parquet output (PyArrow required):

```cmd
.venv\Scripts\python.exe -m pip install -e ".[parquet]"
```

## Quick start

```cmd
datafaker examples\config.yaml
```

That's it — produces CSV files in the current directory.

## CLI reference

```
datafaker <config> [-o OUT] [-f FORMAT] [--seed N] [--chunk-size N] [--dry-run]
```

| Argument | Default | Description |
|---|---|---|
| `config` | *(required)* | Path to the YAML configuration file |
| `-o, --out` | `.` (current directory) | Output directory, or `.duckdb` file path for DuckDB format |
| `-f, --format` | `csv` | Output format: `csv`, `json`, `duckdb`, `parquet` |
| `--seed` | *(none)* | Integer seed for reproducible output |
| `--chunk-size` | `0` | Parquet only: rows per file chunk (`0` = single file per table) |
| `--dry-run` | *(off)* | Validate config and print generation plan without writing output |

## Examples

```cmd
# Minimal — CSV to current directory
datafaker examples\config.yaml

# Dry run to validate config
datafaker examples\config.yaml --dry-run

# CSV to a specific directory with fixed seed
datafaker examples\config.yaml -o out\hr -f csv --seed 42

# JSON output
datafaker examples\config.yaml -f json -o out\json

# DuckDB single-file database
datafaker examples\config.yaml -f duckdb -o out\data.duckdb

# Parquet — single file per table
datafaker examples\config.yaml -f parquet -o out\parquet

# Parquet — 100k-row chunks, one subfolder per table
datafaker examples\stress_test_config.yaml -f parquet -o out\parquet --chunk-size 100000
```

Parquet chunked output creates the following structure:

```
out/parquet/
  patients/
    chunk_00000.parquet
    chunk_00001.parquet
  encounters/
    chunk_00000.parquet
    ...
```

## scenariofaker

`scenariofaker` extends `datafaker` with configurable data quality and temporal scenario behavior for stress-testing ingestion and transformation pipelines:

- **null bursts** — contiguous runs of null values
- **duplicate-key bursts** — contiguous runs of repeated IDs/keys
- **outlier injection** — numeric values multiplied beyond normal range
- **incompleteness** — random missing values and placeholder IDs
- **time-profile** — event timestamps weighted by time-of-day, day-of-week, and seasonal bursts
- **late arrivals** — simulated lag between event time and ingest/arrival time

`scenariofaker` accepts the same CLI arguments as `datafaker`:

```cmd
scenariofaker examples\scenario_config.yaml --dry-run
scenariofaker examples\scenario_config.yaml -f csv -o out\scenario --seed 42
scenariofaker examples\stress_test_scenario_config.yaml -f parquet -o out\stress --chunk-size 100000
```

### Domain preset configs

| File | Description |
|---|---|
| `examples/scenario_config.yaml` | Basic e-commerce scenario |
| `examples/scenario_sales.yaml` | Sales pipeline with seasonality |
| `examples/scenario_support.yaml` | Support ticket queue |
| `examples/scenario_healthcare.yaml` | Patient encounter data |
| `examples/stress_test_config.yaml` | 305k-row API call log (perf / parquet test) |
| `examples/stress_test_scenario_config.yaml` | 300k-row financial transactions with full scenarios |

### Sanity checks

```cmd
.venv\Scripts\python.exe scripts\sanity_checks\sanity_datafaker.py
.venv\Scripts\python.exe scripts\sanity_checks\sanity_scenariofaker.py
```

## YAML configuration reference

### Table structure

```yaml
tables:
  table_name:
    count: 1000
    fields:
      field_name: <field config>
```

### Field types

```yaml
# Sequence (integer primary key)
id:
  sequence:
    start: 1
    step: 1

# Static value
country:
  static: "US"

# Faker provider (with optional args)
name:
  faker: name

age:
  faker: random_int
  min: 18
  max: 90

status:
  faker: random_element
  elements: ["active", "inactive", "pending"]

# Foreign key (referential integrity)
customer_id:
  fk: customers.customer_id

# Weighted random choice
tier:
  weighted_choice:
    values: [bronze, silver, gold]
    weights: [0.6, 0.3, 0.1]
```

### scenariofaker — field scenarios

```yaml
amount:
  faker: pyfloat
  min_value: 10
  max_value: 1000
  positive: true
  scenarios:
    missing_probability: 0.05
    placeholder_values:
      values: [0.0, -1.0]
      probability: 0.02
    outlier:
      probability: 0.01
      multiplier_min: 5.0
      multiplier_max: 20.0
```

### scenariofaker — time profile

```yaml
tables:
  events:
    count: 50000
    time_profile:
      column: event_ts
      start: "2025-01-01T00:00:00"
      end: "2025-12-31T23:59:59"
      day_of_week_weights:
        monday: 1.2
        saturday: 0.5
      hour_weights:
        "9": 2.0
        "17": 1.5
        "3": 0.2
      seasonal_bursts:
        - start: "2025-11-25T00:00:00"
          end: "2025-12-01T23:59:59"
          multiplier: 4.0
```

### scenariofaker — table scenarios

```yaml
tables:
  events:
    scenarios:
      null_bursts:
        - columns: [status, description]
          burst_count: 3
          burst_length_min: 10
          burst_length_max: 50
      duplicate_key_bursts:
        - column: event_id
          burst_count: 2
          burst_length_min: 5
          burst_length_max: 10
      late_arrivals:
        - event_time_column: event_ts
          arrival_time_column: ingest_ts
          probability: 0.05
          min_delay_minutes: 30
          max_delay_minutes: 1440
      incompleteness:
        columns:
          description: 0.10
          category: 0.05
```
