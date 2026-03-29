import argparse
import logging
import os

from .base import load_config


def build_args(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("-c", "--config", required=True, help="YAML configuration file")
    p.add_argument("-o", "--out", default="out", help="Output directory or duckdb file path")
    p.add_argument(
        "-f",
        "--format",
        choices=["csv", "json", "duckdb"],
        default="csv",
        help="Output format",
    )
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible data")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and print generation plan without writing output",
    )
    return p


def run_cli(generator_class, description: str, argv=None) -> None:
    parser = build_args(description)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    cfg = load_config(args.config)
    gen = generator_class(cfg, seed=args.seed)

    if args.dry_run:
        table_names = list(cfg.get("tables", {}).keys())
        print("Config is valid.")
        print(f"Tables to generate ({len(table_names)}): {', '.join(table_names)}")
        return

    gen.generate()
    if args.format == "csv":
        gen.to_csv(args.out)
        print(f"Wrote CSV files to {args.out}")
    elif args.format == "json":
        gen.to_json(args.out)
        print(f"Wrote JSON files to {args.out}")
    elif args.format == "duckdb":
        db_path = args.out
        if os.path.isdir(db_path):
            db_path = os.path.join(db_path, "data.duckdb")
        gen.to_duckdb(db_path)
        print(f"Wrote DuckDB database to {db_path}")
