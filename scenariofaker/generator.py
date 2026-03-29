import logging
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List

import duckdb
import pandas as pd
import yaml
from faker import Faker


logger = logging.getLogger(__name__)

_DAY_NAME_TO_INDEX = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "wednesday": 2,
    "thu": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}


class ScenarioDataGenerator:
    """Generate synthetic tabular data with configurable data quality and time scenarios."""

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
                if isinstance(fconf, dict):
                    if "fk" in fconf:
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

                    scenarios = fconf.get("scenarios", {})
                    if scenarios:
                        if not isinstance(scenarios, dict):
                            raise ValueError(
                                f"Field 'scenarios' for {tname}.{fname} must be a mapping, "
                                f"got {type(scenarios).__name__}"
                            )
                        self._validate_field_scenarios(tname, fname, scenarios)

            time_profile = tconf.get("time_profile")
            if time_profile is not None:
                self._validate_time_profile(tname, time_profile)

            table_scenarios = tconf.get("scenarios", {})
            if table_scenarios:
                self._validate_table_scenarios(tname, table_scenarios)

    def _validate_probability(self, value: float, label: str) -> None:
        if value < 0 or value > 1:
            raise ValueError(f"{label} must be between 0 and 1, got {value}")

    def _validate_field_scenarios(self, tname: str, fname: str, scenarios: Dict[str, Any]) -> None:
        missing_prob = scenarios.get("missing_probability")
        if missing_prob is not None:
            self._validate_probability(float(missing_prob), f"{tname}.{fname} missing_probability")

        placeholders = scenarios.get("placeholder_values")
        if placeholders is not None:
            if not isinstance(placeholders, dict):
                raise ValueError(f"{tname}.{fname} placeholder_values must be an object")
            vals = placeholders.get("values", [])
            if not isinstance(vals, list) or not vals:
                raise ValueError(f"{tname}.{fname} placeholder_values.values must be a non-empty list")
            prob = float(placeholders.get("probability", 0.05))
            self._validate_probability(prob, f"{tname}.{fname} placeholder_values.probability")

        outlier = scenarios.get("outlier")
        if outlier is not None:
            if not isinstance(outlier, dict):
                raise ValueError(f"{tname}.{fname} outlier must be an object")
            prob = float(outlier.get("probability", 0.01))
            self._validate_probability(prob, f"{tname}.{fname} outlier.probability")

    def _validate_time_profile(self, tname: str, profile: Dict[str, Any]) -> None:
        if not isinstance(profile, dict):
            raise ValueError(f"{tname}.time_profile must be an object")

        required = ["column", "start", "end"]
        for key in required:
            if key not in profile:
                raise ValueError(f"{tname}.time_profile missing required key '{key}'")

        start = self._parse_datetime(profile["start"])
        end = self._parse_datetime(profile["end"])
        if end <= start:
            raise ValueError(f"{tname}.time_profile.end must be after start")

        dow = profile.get("day_of_week_weights", {})
        if dow and not isinstance(dow, dict):
            raise ValueError(f"{tname}.time_profile.day_of_week_weights must be an object")

        hours = profile.get("hour_weights", {})
        if hours and not isinstance(hours, dict):
            raise ValueError(f"{tname}.time_profile.hour_weights must be an object")

    def _validate_table_scenarios(self, tname: str, scenarios: Dict[str, Any]) -> None:
        null_bursts = scenarios.get("null_bursts", [])
        if null_bursts and not isinstance(null_bursts, list):
            raise ValueError(f"{tname}.scenarios.null_bursts must be a list")

        for idx, burst in enumerate(null_bursts):
            if not isinstance(burst, dict):
                raise ValueError(f"{tname}.scenarios.null_bursts[{idx}] must be an object")
            columns = burst.get("columns")
            if columns is not None and not isinstance(columns, list):
                raise ValueError(
                    f"{tname}.scenarios.null_bursts[{idx}].columns must be a list"
                )
        duplicate_key_bursts = scenarios.get("duplicate_key_bursts", [])
        if duplicate_key_bursts and not isinstance(duplicate_key_bursts, list):
            raise ValueError(f"{tname}.scenarios.duplicate_key_bursts must be a list")

        for idx, burst in enumerate(duplicate_key_bursts):
            if not isinstance(burst, dict):
                raise ValueError(f"{tname}.scenarios.duplicate_key_bursts[{idx}] must be an object")
            if "column" not in burst:
                raise ValueError(
                    f"{tname}.scenarios.duplicate_key_bursts[{idx}] missing required key 'column'"
                )

        late_arrivals = scenarios.get("late_arrivals", [])
        if late_arrivals and not isinstance(late_arrivals, list):
            raise ValueError(f"{tname}.scenarios.late_arrivals must be a list")

        for idx, rule in enumerate(late_arrivals):
            if not isinstance(rule, dict):
                raise ValueError(f"{tname}.scenarios.late_arrivals[{idx}] must be an object")
            if "event_time_column" not in rule:
                raise ValueError(
                    f"{tname}.scenarios.late_arrivals[{idx}] missing required key 'event_time_column'"
                )
            probability = float(rule.get("probability", 0.05))
            self._validate_probability(probability, f"{tname}.scenarios.late_arrivals[{idx}].probability")

        incompleteness = scenarios.get("incompleteness", {})
        if incompleteness and not isinstance(incompleteness, dict):
            raise ValueError(f"{tname}.scenarios.incompleteness must be an object")

        columns = incompleteness.get("columns", {}) if incompleteness else {}
        if columns and not isinstance(columns, dict):
            raise ValueError(f"{tname}.scenarios.incompleteness.columns must be an object")

        for col_name, prob in columns.items():
            self._validate_probability(float(prob), f"{tname}.scenarios.incompleteness.columns.{col_name}")

    def _parse_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(f"Invalid datetime value: {value!r}. Use ISO format.") from exc
        raise ValueError(f"Datetime value must be string or datetime, got {type(value)}")

    def _extract_refs(self) -> Dict[str, List[str]]:
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
        incoming = {t: 0 for t in deps}
        for table, depends_on in deps.items():
            for dep in depends_on:
                if dep not in incoming:
                    raise ValueError(f"Unknown dependency table referenced: {dep}")
                incoming[table] += 1

        queue = [t for t, cnt in incoming.items() if cnt == 0]
        order: List[str] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for table, depends_on in deps.items():
                if node in depends_on:
                    incoming[table] -= 1
                    if incoming[table] == 0:
                        queue.append(table)

        if len(order) != len(deps):
            raise ValueError("Cycle detected in table foreign-key dependencies")
        return order

    def _run_faker_provider(self, provider: str, fconf: Dict[str, Any]) -> Any:
        if not hasattr(self.faker, provider):
            raise ValueError(f"Unknown faker provider: {provider}")

        func = getattr(self.faker, provider)
        skip_keys = {
            "faker",
            "fk",
            "sequence",
            "static",
            "scenarios",
            "weighted_choice",
            "weights",
            "values",
        }
        kwargs = {k: v for k, v in fconf.items() if k not in skip_keys}
        try:
            return func(**kwargs)
        except TypeError as exc:
            if kwargs:
                raise ValueError(
                    f"Invalid arguments for faker provider '{provider}': {kwargs}. Error: {exc}"
                ) from exc
            raise ValueError(
                f"Faker provider '{provider}' requires arguments. Supply them in field config."
            ) from exc

    def _choose_weighted_value(self, values: List[Any], weights: List[float]) -> Any:
        if len(values) != len(weights):
            raise ValueError("weighted choice requires equal-length 'values' and 'weights'")
        if not values:
            raise ValueError("weighted choice requires non-empty 'values'")
        if any(w < 0 for w in weights):
            raise ValueError("weighted choice requires non-negative 'weights'")
        total_weight = sum(weights)
        if total_weight <= 0:
            raise ValueError("weighted choice requires total weight > 0")
        return random.choices(values, weights=weights, k=1)[0]

    def _apply_field_scenarios(self, value: Any, fconf: Dict[str, Any]) -> Any:
        scenarios = fconf.get("scenarios", {})
        if not scenarios:
            return value

        missing_probability = scenarios.get("missing_probability")
        if missing_probability is not None and random.random() < float(missing_probability):
            return None

        placeholder_conf = scenarios.get("placeholder_values")
        if isinstance(placeholder_conf, dict):
            prob = float(placeholder_conf.get("probability", 0.05))
            if random.random() < prob:
                values = placeholder_conf.get("values", [])
                if values:
                    return random.choice(values)

        outlier_conf = scenarios.get("outlier")
        if isinstance(outlier_conf, dict) and value is not None and isinstance(value, (int, float)):
            prob = float(outlier_conf.get("probability", 0.01))
            if random.random() < prob:
                mult_min = float(outlier_conf.get("multiplier_min", 3.0))
                mult_max = float(outlier_conf.get("multiplier_max", 10.0))
                factor = random.uniform(mult_min, mult_max)
                value = value * factor
                if isinstance(value, float) and outlier_conf.get("round") is not None:
                    value = round(value, int(outlier_conf.get("round")))

        return value

    def _generate_field(self, fconf: Any, row_idx: int, _current_row: Dict[str, Any]) -> Any:
        if isinstance(fconf, dict):
            if "static" in fconf:
                value = fconf["static"]
                return self._apply_field_scenarios(value, fconf)

            if "sequence" in fconf:
                start = int(fconf["sequence"].get("start", 1))
                step = int(fconf["sequence"].get("step", 1))
                value = start + row_idx * step
                return self._apply_field_scenarios(value, fconf)

            if "weighted_choice" in fconf:
                wc = fconf["weighted_choice"]
                if not isinstance(wc, dict):
                    raise ValueError("weighted_choice must be an object with 'values' and 'weights'")
                value = self._choose_weighted_value(wc.get("values", []), wc.get("weights", []))
                return self._apply_field_scenarios(value, fconf)

            if "values" in fconf and "weights" in fconf:
                value = self._choose_weighted_value(fconf["values"], fconf["weights"])
                return self._apply_field_scenarios(value, fconf)

            if "faker" in fconf:
                value = self._run_faker_provider(fconf["faker"], fconf)
                return self._apply_field_scenarios(value, fconf)

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
                    raise ValueError(f"Referenced table {cache_key} has no rows")
                value = random.choice(choices)
                return self._apply_field_scenarios(value, fconf)

        return fconf

    def _normalize_day_weights(self, config: Dict[str, Any]) -> Dict[int, float]:
        weights = {i: 1.0 for i in range(7)}
        for key, val in config.items():
            idx = None
            if isinstance(key, int) or (isinstance(key, str) and key.isdigit()):
                idx = int(key)
            elif isinstance(key, str):
                idx = _DAY_NAME_TO_INDEX.get(key.strip().lower())
            if idx is None or idx < 0 or idx > 6:
                raise ValueError(f"Invalid day_of_week key: {key!r}")
            weights[idx] = float(val)
        return weights

    def _normalize_hour_weights(self, config: Dict[str, Any]) -> Dict[int, float]:
        weights = {i: 1.0 for i in range(24)}
        for key, val in config.items():
            if isinstance(key, int) or (isinstance(key, str) and key.isdigit()):
                hour = int(key)
            else:
                raise ValueError(f"Invalid hour key: {key!r}")
            if hour < 0 or hour > 23:
                raise ValueError(f"Hour key must be between 0 and 23, got {hour}")
            weights[hour] = float(val)
        return weights

    def _generate_weighted_timestamps(self, count: int, profile: Dict[str, Any]) -> List[datetime]:
        start = self._parse_datetime(profile["start"])
        end = self._parse_datetime(profile["end"])
        if end <= start:
            raise ValueError("time_profile.end must be after start")

        day_weights = self._normalize_day_weights(profile.get("day_of_week_weights", {}))
        hour_weights = self._normalize_hour_weights(profile.get("hour_weights", {}))

        burst_windows = []
        for burst in profile.get("seasonal_bursts", []):
            bstart = self._parse_datetime(burst["start"])
            bend = self._parse_datetime(burst["end"])
            mult = float(burst.get("multiplier", 1.0))
            burst_windows.append((bstart, bend, mult))

        max_day = max(day_weights.values()) if day_weights else 1.0
        max_hour = max(hour_weights.values()) if hour_weights else 1.0
        max_burst = 1.0
        for _, _, mult in burst_windows:
            if mult > max_burst:
                max_burst = mult
        max_weight = max_day * max_hour * max_burst
        if max_weight <= 0:
            max_weight = 1.0

        total_seconds = int((end - start).total_seconds())
        if total_seconds <= 0:
            raise ValueError("time_profile range must span at least one second")

        results: List[datetime] = []
        for _ in range(count):
            accepted = None
            for _attempt in range(10000):
                candidate = start + timedelta(seconds=random.randint(0, total_seconds))
                weight = day_weights[candidate.weekday()] * hour_weights[candidate.hour]
                for bstart, bend, mult in burst_windows:
                    if bstart <= candidate <= bend:
                        weight *= mult
                if weight <= 0:
                    continue
                if random.random() < (weight / max_weight):
                    accepted = candidate
                    break
            if accepted is None:
                accepted = start + timedelta(seconds=random.randint(0, total_seconds))
            results.append(accepted)
        return results

    def _apply_table_scenarios(self, tname: str, tconf: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
        scenarios = tconf.get("scenarios", {})
        if not scenarios or df.empty:
            return df

        null_bursts = scenarios.get("null_bursts", [])
        for burst in null_bursts:
            columns = burst.get("columns", [])
            if not columns:
                continue

            burst_count = int(burst.get("burst_count", 1))
            length_min = int(burst.get("burst_length_min", burst.get("length", 5)))
            length_max = int(burst.get("burst_length_max", burst.get("length", 5)))
            value = burst.get("value", None)
            if length_max < length_min:
                length_max = length_min

            for _ in range(max(0, burst_count)):
                start_idx = random.randint(0, len(df) - 1)
                length = random.randint(max(1, length_min), max(1, length_max))
                end_idx = min(len(df), start_idx + length)
                for col in columns:
                    if col in df.columns:
                        df.loc[start_idx:end_idx - 1, col] = value
                    else:
                        logger.warning("Ignoring null burst column '%s' in table '%s' (column missing)", col, tname)

        incompleteness = scenarios.get("incompleteness", {})
        columns = incompleteness.get("columns", {}) if isinstance(incompleteness, dict) else {}
        for col, prob in columns.items():
            if col not in df.columns:
                logger.warning("Ignoring incompleteness column '%s' in table '%s' (column missing)", col, tname)
                continue
            probability = float(prob)
            mask = [random.random() < probability for _ in range(len(df))]
            df.loc[mask, col] = None

        duplicate_key_bursts = scenarios.get("duplicate_key_bursts", [])
        for burst in duplicate_key_bursts:
            column = burst.get("column")
            if column not in df.columns:
                logger.warning(
                    "Ignoring duplicate key burst column '%s' in table '%s' (column missing)",
                    column,
                    tname,
                )
                continue

            burst_count = int(burst.get("burst_count", 1))
            length_min = int(burst.get("burst_length_min", burst.get("length", 3)))
            length_max = int(burst.get("burst_length_max", burst.get("length", 5)))
            if length_max < length_min:
                length_max = length_min

            for _ in range(max(0, burst_count)):
                if len(df) == 0:
                    break
                source_idx = random.randint(0, len(df) - 1)
                repeated_value = df.iloc[source_idx][column]
                start_idx = random.randint(0, len(df) - 1)
                length = random.randint(max(2, length_min), max(2, length_max))
                end_idx = min(len(df), start_idx + length)
                df.loc[start_idx:end_idx - 1, column] = repeated_value

        late_arrivals = scenarios.get("late_arrivals", [])
        for rule in late_arrivals:
            event_col = rule.get("event_time_column")
            if event_col not in df.columns:
                logger.warning(
                    "Ignoring late arrivals rule in table '%s' because event column '%s' is missing",
                    tname,
                    event_col,
                )
                continue

            arrival_col = rule.get("arrival_time_column")
            probability = float(rule.get("probability", 0.05))
            min_delay_minutes = int(rule.get("min_delay_minutes", 60))
            max_delay_minutes = int(rule.get("max_delay_minutes", 24 * 60))
            if max_delay_minutes < min_delay_minutes:
                max_delay_minutes = min_delay_minutes

            event_series = pd.to_datetime(df[event_col], errors="coerce")
            if arrival_col:
                df[arrival_col] = event_series

            for idx in range(len(df)):
                event_time = event_series.iloc[idx]
                if pd.isna(event_time):
                    continue
                if random.random() >= probability:
                    continue
                delay = random.randint(min_delay_minutes, max_delay_minutes)
                delayed_arrival = event_time + timedelta(minutes=delay)
                if arrival_col:
                    df.at[idx, arrival_col] = delayed_arrival
                else:
                    df.at[idx, event_col] = event_time - timedelta(minutes=delay)

        return df

    def generate(self) -> Dict[str, pd.DataFrame]:
        tables_conf = self.config.get("tables", {})
        deps = self._extract_refs()
        order = self._topo_sort(deps)

        for tname in order:
            tconf = tables_conf[tname]
            count = int(tconf.get("count", 0))
            fields = tconf.get("fields", {})
            logger.info("Generating table '%s' (%s rows)", tname, count)

            time_profile = tconf.get("time_profile")
            time_values: List[datetime] = []
            time_column = None
            if time_profile is not None:
                time_column = str(time_profile["column"])
                time_values = self._generate_weighted_timestamps(count, time_profile)

            rows: List[Dict[str, Any]] = []
            for i in range(count):
                row: Dict[str, Any] = {}
                for fname, fconf in fields.items():
                    row[fname] = self._generate_field(fconf, i, row)
                if time_column is not None:
                    row[time_column] = time_values[i]
                rows.append(row)

            df = pd.DataFrame(rows)
            df = self._apply_table_scenarios(tname, tconf, df)
            self.tables[tname] = df
            self._fk_cache.clear()
            logger.info("Finished table '%s' with %s rows", tname, len(df))

        return self.tables

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
                    f.write(df.to_json(orient="records", indent=2, date_format="iso"))
            except OSError as exc:
                raise OSError(f"Failed to write JSON output for table '{tname}' to '{path}'") from exc

    def to_duckdb(self, db_path: str) -> None:
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
