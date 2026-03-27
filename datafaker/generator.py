import random
import os
import json
import logging
from typing import Dict, Any, List

import yaml
import pandas as pd
from faker import Faker
import duckdb


logger = logging.getLogger(__name__)


class DataGenerator:
    """Generate synthetic tabular data from YAML config with FK integrity."""

    def __init__(self, config: Dict[str, Any], seed: int | None = None):
        self.config = config
        self.seed = seed
        self.faker = Faker()
        if seed is not None:
            random.seed(seed)
            self.faker.seed_instance(seed)
        self.tables: Dict[str, pd.DataFrame] = {}
        self._fk_cache: Dict[str, List[Any]] = {}
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate top-level config shape and foreign-key references."""
        tables_conf = self.config.get("tables")
        if not isinstance(tables_conf, dict) or not tables_conf:
            raise ValueError("Config must include a non-empty 'tables' mapping")

        for tname, tconf in tables_conf.items():
            if not isinstance(tconf, dict):
                raise ValueError(f"Table '{tname}' must map to an object")
            fields = tconf.get("fields")
            if not isinstance(fields, dict) or not fields:
                raise ValueError(f"Table '{tname}' must include a non-empty 'fields' mapping")

            for fname, fconf in fields.items():
                if isinstance(fconf, dict) and "fk" in fconf:
                    fk = fconf["fk"]
                    if not isinstance(fk, str) or "." not in fk:
                        raise ValueError(
                            f"Invalid FK format for {tname}.{fname}: expected 'table.field', got {fk!r}"
                        )
                    ref_table, ref_field = fk.split(".", 1)
                    if ref_table not in tables_conf:
                        raise ValueError(
                            f"FK in {tname}.{fname} references unknown table '{ref_table}'"
                        )
                    ref_fields = tables_conf[ref_table].get("fields", {})
                    if ref_field not in ref_fields:
                        raise ValueError(
                            f"FK in {tname}.{fname} references unknown field '{ref_table}.{ref_field}'"
                        )

    def _extract_refs(self) -> Dict[str, List[str]]:
        # build dependency graph: table -> list of tables it depends on
        tables = self.config.get("tables", {})
        deps: Dict[str, List[str]] = {t: [] for t in tables}
        for tname, tconf in tables.items():
            fields = tconf.get("fields", {})
            for _fname, fconf in fields.items():
                if isinstance(fconf, dict) and "fk" in fconf:
                    ref = fconf["fk"]  # expected format: other_table.other_field
                    ref_table = ref.split(".")[0]
                    if ref_table not in deps[tname]:
                        deps[tname].append(ref_table)
        return deps

    def _topo_sort(self, deps: Dict[str, List[str]]) -> List[str]:
        # Kahn's algorithm
        incoming = {t: 0 for t in deps}
        for _t, ds in deps.items():
            for d in ds:
                if d not in incoming:
                    raise ValueError(f"Unknown dependency table referenced: {d}")
                incoming[_t] += 1
        # nodes with zero incoming
        zero = [t for t, cnt in incoming.items() if cnt == 0]
        order: List[str] = []
        while zero:
            n = zero.pop(0)
            order.append(n)
            for m, ds in deps.items():
                if n in ds:
                    incoming[m] -= 1
                    if incoming[m] == 0:
                        zero.append(m)
        if len(order) != len(deps):
            raise ValueError("Cycle detected in table foreign-key dependencies; please break cycles or pre-generate keys.")
        return order

    def _run_faker_provider(self, provider: str, fconf: Dict[str, Any]) -> Any:
        """Invoke faker provider with optional keyword args from config.

        Example:
          faker: random_int
          min: 1
          max: 10
        """
        if not hasattr(self.faker, provider):
            raise ValueError(f"Unknown faker provider: {provider}")

        func = getattr(self.faker, provider)
        kwargs = {k: v for k, v in fconf.items() if k not in {"faker", "fk", "sequence", "static"}}
        try:
            return func(**kwargs)
        except TypeError as exc:
            if kwargs:
                raise ValueError(
                    f"Invalid arguments for faker provider '{provider}': {kwargs}. Error: {exc}"
                ) from exc
            raise ValueError(
                f"Faker provider '{provider}' requires arguments. Supply them in the field config."
            ) from exc

    def _generate_field(self, fconf: Any, row_idx: int, current_row: Dict[str, Any]) -> Any:
        # fconf can be: dict with keys 'faker', 'sequence', 'static', 'fk'
        if isinstance(fconf, dict):
            if "static" in fconf:
                return fconf["static"]
            if "sequence" in fconf:
                start = int(fconf["sequence"].get("start", 1))
                step = int(fconf["sequence"].get("step", 1))
                return start + row_idx * step
            if "faker" in fconf:
                provider = fconf["faker"]
                return self._run_faker_provider(provider, fconf)
            if "fk" in fconf:
                ref = fconf["fk"]
                ref_table, ref_field = ref.split(".")
                if ref_table not in self.tables:
                    raise ValueError(f"Referenced table {ref_table} not generated yet")
                cache_key = f"{ref_table}.{ref_field}"
                choices = self._fk_cache.get(cache_key)
                if choices is None:
                    choices = self.tables[ref_table][ref_field].tolist()
                    self._fk_cache[cache_key] = choices
                if not choices:
                    raise ValueError(f"Referenced table {ref_table}.{ref_field} has no rows to choose from")
                return random.choice(choices)
        # fallback: literal
        return fconf

    def generate(self) -> Dict[str, pd.DataFrame]:
        """Generate all configured tables in FK-safe order and return them."""
        tables_conf = self.config.get("tables", {})
        deps = self._extract_refs()
        order = self._topo_sort(deps)
        for tname in order:
            tconf = tables_conf[tname]
            count = int(tconf.get("count", 0))
            fields = tconf.get("fields", {})
            logger.info("Generating table '%s' (%s rows)", tname, count)
            rows: List[Dict[str, Any]] = []
            for i in range(count):
                row: Dict[str, Any] = {}
                for fname, fconf in fields.items():
                    value = self._generate_field(fconf, i, row)
                    row[fname] = value
                rows.append(row)
            df = pd.DataFrame(rows)
            self.tables[tname] = df
            logger.info("Finished table '%s' with %s rows", tname, len(df))
            # Reference tables are immutable after creation; clear to bound cache size.
            self._fk_cache.clear()
        return self.tables

    def to_csv(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        for tname, df in self.tables.items():
            path = os.path.join(out_dir, f"{tname}.csv")
            df.to_csv(path, index=False)

    def to_json(self, out_dir: str):
        os.makedirs(out_dir, exist_ok=True)
        for tname, df in self.tables.items():
            path = os.path.join(out_dir, f"{tname}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(df.to_json(orient="records", indent=2))
            except OSError as exc:
                raise OSError(f"Failed to write JSON output for table '{tname}' to '{path}'") from exc

    def to_duckdb(self, db_path: str):
        # create duckdb file and write each dataframe as a table
        con = duckdb.connect(db_path)
        for tname, df in self.tables.items():
            temp = f"__tmp_{tname}"
            con.register(temp, df)
            con.execute(f"CREATE TABLE {tname} AS SELECT * FROM {temp}")
            con.unregister(temp)
        con.close()


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
