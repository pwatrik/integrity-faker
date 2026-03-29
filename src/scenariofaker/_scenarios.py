import logging
import random
from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd


logger = logging.getLogger(__name__)


def validate_probability(value: float, label: str) -> None:
    if value < 0 or value > 1:
        raise ValueError(f"{label} must be between 0 and 1, got {value}")


def validate_field_scenarios(tname: str, fname: str, scenarios: Dict[str, Any]) -> None:
    missing_prob = scenarios.get("missing_probability")
    if missing_prob is not None:
        validate_probability(float(missing_prob), f"{tname}.{fname} missing_probability")

    placeholders = scenarios.get("placeholder_values")
    if placeholders is not None:
        if not isinstance(placeholders, dict):
            raise ValueError(f"{tname}.{fname} placeholder_values must be an object")
        vals = placeholders.get("values", [])
        if not isinstance(vals, list) or not vals:
            raise ValueError(f"{tname}.{fname} placeholder_values.values must be a non-empty list")
        prob = float(placeholders.get("probability", 0.05))
        validate_probability(prob, f"{tname}.{fname} placeholder_values.probability")

    outlier = scenarios.get("outlier")
    if outlier is not None:
        if not isinstance(outlier, dict):
            raise ValueError(f"{tname}.{fname} outlier must be an object")
        prob = float(outlier.get("probability", 0.01))
        validate_probability(prob, f"{tname}.{fname} outlier.probability")


def validate_time_profile(tname: str, profile: Dict[str, Any]) -> None:
    from ._time_profile import parse_datetime

    if not isinstance(profile, dict):
        raise ValueError(f"{tname}.time_profile must be an object")

    for key in ("column", "start", "end"):
        if key not in profile:
            raise ValueError(f"{tname}.time_profile missing required key '{key}'")

    start = parse_datetime(profile["start"])
    end = parse_datetime(profile["end"])
    if end <= start:
        raise ValueError(f"{tname}.time_profile.end must be after start")

    dow = profile.get("day_of_week_weights", {})
    if dow and not isinstance(dow, dict):
        raise ValueError(f"{tname}.time_profile.day_of_week_weights must be an object")

    hours = profile.get("hour_weights", {})
    if hours and not isinstance(hours, dict):
        raise ValueError(f"{tname}.time_profile.hour_weights must be an object")

    seasonal_bursts = profile.get("seasonal_bursts", [])
    if seasonal_bursts:
        if not isinstance(seasonal_bursts, list):
            raise ValueError(f"{tname}.time_profile.seasonal_bursts must be a list")
        for idx, burst in enumerate(seasonal_bursts):
            if not isinstance(burst, dict):
                raise ValueError(
                    f"{tname}.time_profile.seasonal_bursts[{idx}] must be an object"
                )
            for key in ("start", "end"):
                if key not in burst:
                    raise ValueError(
                        f"{tname}.time_profile.seasonal_bursts[{idx}] missing required key '{key}'"
                    )
            burst_start = parse_datetime(burst["start"])
            burst_end = parse_datetime(burst["end"])
            if burst_end <= burst_start:
                raise ValueError(
                    f"{tname}.time_profile.seasonal_bursts[{idx}].end must be after start"
                )
            multiplier = burst.get("multiplier", 1.0)
            try:
                multiplier_value = float(multiplier)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{tname}.time_profile.seasonal_bursts[{idx}].multiplier must be a number"
                )
            if multiplier_value <= 0:
                raise ValueError(
                    f"{tname}.time_profile.seasonal_bursts[{idx}].multiplier must be greater than 0"
                )


def validate_table_scenarios(tname: str, scenarios: Dict[str, Any]) -> None:
    null_bursts = scenarios.get("null_bursts", [])
    if null_bursts and not isinstance(null_bursts, list):
        raise ValueError(f"{tname}.scenarios.null_bursts must be a list")
    for idx, burst in enumerate(null_bursts):
        if not isinstance(burst, dict):
            raise ValueError(f"{tname}.scenarios.null_bursts[{idx}] must be an object")
        columns = burst.get("columns")
        if columns is not None and not isinstance(columns, list):
            raise ValueError(f"{tname}.scenarios.null_bursts[{idx}].columns must be a list")

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
        validate_probability(
            float(rule.get("probability", 0.05)),
            f"{tname}.scenarios.late_arrivals[{idx}].probability",
        )

    incompleteness = scenarios.get("incompleteness", {})
    if incompleteness and not isinstance(incompleteness, dict):
        raise ValueError(f"{tname}.scenarios.incompleteness must be an object")
    columns = incompleteness.get("columns", {}) if incompleteness else {}
    if columns and not isinstance(columns, dict):
        raise ValueError(f"{tname}.scenarios.incompleteness.columns must be an object")
    for col_name, prob in columns.items():
        validate_probability(
            float(prob), f"{tname}.scenarios.incompleteness.columns.{col_name}"
        )


def choose_weighted_value(values: List[Any], weights: List[float]) -> Any:
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


def apply_field_scenarios(value: Any, fconf: Dict[str, Any]) -> Any:
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


def apply_table_scenarios(
    tname: str, tconf: Dict[str, Any], df: pd.DataFrame
) -> pd.DataFrame:
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
                    df.loc[start_idx : end_idx - 1, col] = value
                else:
                    logger.warning(
                        "Ignoring null burst column '%s' in table '%s' (column missing)",
                        col,
                        tname,
                    )

    incompleteness = scenarios.get("incompleteness", {})
    inc_columns = incompleteness.get("columns", {}) if isinstance(incompleteness, dict) else {}
    for col, prob in inc_columns.items():
        if col not in df.columns:
            logger.warning(
                "Ignoring incompleteness column '%s' in table '%s' (column missing)", col, tname
            )
            continue
        mask = [random.random() < float(prob) for _ in range(len(df))]
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
            df.loc[start_idx : end_idx - 1, column] = repeated_value

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
        arrival_col = rule.get("arrival_time_column") or "ingest_ts"
        probability = float(rule.get("probability", 0.05))
        min_delay_minutes = int(rule.get("min_delay_minutes", 60))
        max_delay_minutes = int(rule.get("max_delay_minutes", 24 * 60))
        if max_delay_minutes < min_delay_minutes:
            max_delay_minutes = min_delay_minutes
        event_series = pd.to_datetime(df[event_col], errors="coerce")
        df[arrival_col] = event_series
        for idx in range(len(df)):
            event_time = event_series.iloc[idx]
            if pd.isna(event_time):
                continue
            if random.random() >= probability:
                continue
            delay = random.randint(min_delay_minutes, max_delay_minutes)
            df.at[idx, arrival_col] = event_time + timedelta(minutes=delay)

    return df
