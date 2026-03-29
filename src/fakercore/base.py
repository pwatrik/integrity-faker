import os
import random
import logging
import time
from typing import Any, ClassVar, Dict, List

import duckdb
import pandas as pd
import yaml
from faker import Faker
from tqdm import tqdm


logger = logging.getLogger(__name__)


class BaseDataGenerator:
    """Base class for synthetic tabular data generation from YAML config.

    Subclasses extend this via three template hooks:
      _pre_generate_table(tname, tconf, count)  — called once before the row loop
      _generate_row_extras(row_idx, tconf)       — called per row; merge returned dict into row
      _post_generate_table(tname, tconf, df)     — called after row loop; transform/return df
    """

    _FAKER_SKIP_KEYS: ClassVar[frozenset] = frozenset({"faker", "fk", "sequence", "static"})

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
        """Build dependency graph: table -> list of tables it depends on via FK."""
        tables = self.config.get("tables", {})
        deps: Dict[str, List[str]] = {t: [] for t in tables}
        for tname, tconf in tables.items():
            fields = tconf.get("fields", {})
            for _fname, fconf in fields.items():
                if isinstance(fconf, dict) and "fk" in fconf:
                    ref_table = fconf["fk"].split(".", 1)[0]
                    if ref_table not in deps[tname]:
                        deps[tname].append(ref_table)
        return deps

    def _topo_sort(self, deps: Dict[str, List[str]]) -> List[str]:
        """Kahn's algorithm topological sort."""
        incoming = {t: 0 for t in deps}
        for _t, ds in deps.items():
            for d in ds:
                if d not in incoming:
                    raise ValueError(f"Unknown dependency table referenced: {d}")
                incoming[_t] += 1
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
            raise ValueError(
                "Cycle detected in table foreign-key dependencies; "
                "please break cycles or pre-generate keys."
            )
        return order

    def _run_faker_provider(self, provider: str, fconf: Dict[str, Any]) -> Any:
        """Invoke a Faker provider with optional keyword args; skip keys in _FAKER_SKIP_KEYS."""
        if not hasattr(self.faker, provider):
            raise ValueError(f"Unknown faker provider: {provider}")
        func = getattr(self.faker, provider)
        kwargs = {k: v for k, v in fconf.items() if k not in self._FAKER_SKIP_KEYS}
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
        """Generate a single field value. Subclasses may extend for additional field types."""
        if isinstance(fconf, dict):
            if "static" in fconf:
                return fconf["static"]
            if "sequence" in fconf:
                start = int(fconf["sequence"].get("start", 1))
                step = int(fconf["sequence"].get("step", 1))
                return start + row_idx * step
            if "faker" in fconf:
                return self._run_faker_provider(fconf["faker"], fconf)
            if "fk" in fconf:
                ref_table, ref_field = fconf["fk"].split(".", 1)
                if ref_table not in self.tables:
                    raise ValueError(f"Referenced table {ref_table} not generated yet")
                cache_key = f"{ref_table}.{ref_field}"
                choices = self._fk_cache.get(cache_key)
                if choices is None:
                    choices = self.tables[ref_table][ref_field].tolist()
                    self._fk_cache[cache_key] = choices
                if not choices:
                    raise ValueError(
                        f"Referenced table {ref_table}.{ref_field} has no rows to choose from"
                    )
                return random.choice(choices)
        return fconf

    # ------------------------------------------------------------------
    # Template hooks — override in subclasses; all are no-ops by default
    # ------------------------------------------------------------------

    def _pre_generate_table(self, tname: str, tconf: Dict[str, Any], count: int) -> None:
        """Called once before the row loop for each table. Use to precompute per-table state."""

    def _generate_row_extras(self, row_idx: int, tconf: Dict[str, Any]) -> Dict[str, Any]:
        """Called per row; returned dict is merged into the row. Returns {} by default."""
        return {}

    def _post_generate_table(
        self, tname: str, tconf: Dict[str, Any], df: pd.DataFrame
    ) -> pd.DataFrame:
        """Called after the row loop. May transform and return the DataFrame. No-op by default."""
        return df

    # ------------------------------------------------------------------
    # Main generation loop
    # ------------------------------------------------------------------

    def generate(self) -> Dict[str, pd.DataFrame]:
        """Generate all configured tables in FK-safe order and return them."""
        generation_start = time.perf_counter()
        tables_conf = self.config.get("tables", {})
        deps = self._extract_refs()
        order = self._topo_sort(deps)
        for tname in order:
            table_start = time.perf_counter()
            tconf = tables_conf[tname]
            count = int(tconf.get("count", 0))
            fields = tconf.get("fields", {})
            logger.info("Generating table '%s' (%s rows)", tname, count)
            self._pre_generate_table(tname, tconf, count)
            rows: List[Dict[str, Any]] = []
            progress_iter = tqdm(
                range(count),
                desc=f"{tname}",
                unit="row",
                leave=False,
                dynamic_ncols=True,
            )
            for i in progress_iter:
                row: Dict[str, Any] = {}
                for fname, fconf in fields.items():
                    row[fname] = self._generate_field(fconf, i, row)
                row.update(self._generate_row_extras(i, tconf))
                rows.append(row)
            progress_iter.close()
            df = pd.DataFrame(rows)
            df = self._post_generate_table(tname, tconf, df)
            self.tables[tname] = df
            self._fk_cache.clear()
            table_elapsed = time.perf_counter() - table_start
            logger.info(
                "Finished table '%s' with %s rows in %.2fs", tname, len(df), table_elapsed
            )
        total_elapsed = time.perf_counter() - generation_start
        logger.info("Generation complete: %s table(s) in %.2fs", len(order), total_elapsed)
        return self.tables

    # ------------------------------------------------------------------
    # Output writers
    # ------------------------------------------------------------------

    def to_csv(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        for tname, df in self.tables.items():
            path = os.path.join(out_dir, f"{tname}.csv")
            df.to_csv(path, index=False)

    def to_json(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        for tname, df in self.tables.items():
            path = os.path.join(out_dir, f"{tname}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(df.to_json(orient="records", indent=2))
            except OSError as exc:
                raise OSError(
                    f"Failed to write JSON output for table '{tname}' to '{path}'"
                ) from exc

    def to_duckdb(self, db_path: str) -> None:
        con = duckdb.connect(db_path)
        for tname, df in self.tables.items():
            temp = f"__tmp_{tname}"
            con.register(temp, df)
            con.execute(f"CREATE TABLE {tname} AS SELECT * FROM {temp}")
            con.unregister(temp)
        con.close()

    def to_parquet(
        self, out_dir: str, chunk_size: int = 0, compression: str = "snappy"
    ) -> None:
        """Write tables to Parquet format.

        Args:
            out_dir: Output directory
            chunk_size: If >0, splits each table into chunks of this size into separate files
                       (e.g., table_name/chunk_00000.parquet, chunk_00001.parquet, ...).
                       Requires PyArrow. If 0, writes a single file per table.
            compression: Compression codec: 'snappy' (default), 'gzip', 'brotli', 'lz4', 'zstd', or None.
        """
        os.makedirs(out_dir, exist_ok=True)

        if chunk_size > 0:
            try:
                import pyarrow.parquet as pq
            except ImportError as exc:
                raise ImportError(
                    "PyArrow is required for chunked Parquet output. "
                    "Install it with: pip install pyarrow\n"
                    "Or: pip install datafaker[parquet]"
                ) from exc

            for tname, df in self.tables.items():
                table_dir = os.path.join(out_dir, tname)
                os.makedirs(table_dir, exist_ok=True)

                # Split DataFrame into chunks
                num_chunks = (len(df) + chunk_size - 1) // chunk_size
                for chunk_idx in range(num_chunks):
                    start_row = chunk_idx * chunk_size
                    end_row = min((chunk_idx + 1) * chunk_size, len(df))
                    chunk_df = df.iloc[start_row:end_row]

                    chunk_filename = os.path.join(table_dir, f"chunk_{chunk_idx:05d}.parquet")
                    chunk_df.to_parquet(chunk_filename, compression=compression, index=False)
                    logger.info(
                        "Wrote %s rows to %s", len(chunk_df), chunk_filename
                    )
        else:
            # Single file per table
            for tname, df in self.tables.items():
                path = os.path.join(out_dir, f"{tname}.parquet")
                try:
                    df.to_parquet(path, compression=compression, index=False)
                except ImportError as exc:
                    raise ImportError(
                        "PyArrow (or another pandas-compatible Parquet engine) is required "
                        "for Parquet output. Install it with: pip install pyarrow\n"
                        "Or: pip install datafaker[parquet]"
                    ) from exc
                logger.info("Wrote %s rows to %s", len(df), path)


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
