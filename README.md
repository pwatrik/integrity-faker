# YAML-driven Synthetic Data Generator

This small Python utility generates synthetic tabular datasets driven by a YAML configuration. It supports referential integrity between tables (foreign keys) and can output data as CSV files, JSON files, or a DuckDB database.

Quick features

- YAML configuration for tables and fields
- Faker-based values for common personal / date / text data
- Sequence generators for primary keys
- Referential integrity via `fk: other_table.field`
- Config validation with helpful error messages for invalid FK references
- Dry-run mode to validate and preview planned table generation
- Output: CSV (one file per table), JSON (one file per table), or DuckDB (tables inside a .duckdb file)

Usage

Create a venv, install requirements, and run the smoke test:

```cmd
cd C:\Users\Patrick\code\portfolio\datafaker
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe run_smoke.py
```

Example config is at `examples/config.yaml`.

CLI examples

```cmd
.venv\Scripts\python.exe -m datafaker -c examples\config.yaml --dry-run
.venv\Scripts\python.exe -m datafaker -c examples\config.yaml -f csv -o out --seed 42
```

Field configuration tips

- Faker providers can accept arguments directly from YAML:

```yaml
age:
 faker: random_int
 min: 18
 max: 90

status:
 faker: random_element
 elements: ["active", "inactive", "pending"]
```
