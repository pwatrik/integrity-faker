import random
from datetime import datetime, timedelta
from typing import Any, Dict, List


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


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"Invalid datetime value: {value!r}. Use ISO format.") from exc
    raise ValueError(f"Datetime value must be string or datetime, got {type(value)}")


def normalize_day_weights(config: Dict[str, Any]) -> Dict[int, float]:
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


def normalize_hour_weights(config: Dict[str, Any]) -> Dict[int, float]:
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


def generate_weighted_timestamps(count: int, profile: Dict[str, Any]) -> List[datetime]:
    start = parse_datetime(profile["start"])
    end = parse_datetime(profile["end"])
    if end <= start:
        raise ValueError("time_profile.end must be after start")

    day_weights = normalize_day_weights(profile.get("day_of_week_weights", {}))
    hour_weights = normalize_hour_weights(profile.get("hour_weights", {}))

    burst_windows = []
    for burst in profile.get("seasonal_bursts", []):
        bstart = parse_datetime(burst["start"])
        bend = parse_datetime(burst["end"])
        mult = float(burst.get("multiplier", 1.0))
        burst_windows.append((bstart, bend, mult))

    max_day = max(day_weights.values()) if day_weights else 1.0
    max_hour = max(hour_weights.values()) if hour_weights else 1.0
    max_burst = max((mult for _, _, mult in burst_windows), default=1.0)
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
