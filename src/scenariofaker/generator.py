import logging
import os
from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd

from fakercore.base import BaseDataGenerator, load_config
from ._time_profile import generate_weighted_timestamps
from ._scenarios import (
    apply_field_scenarios,
    apply_table_scenarios,
    choose_weighted_value,
    validate_field_scenarios,
    validate_table_scenarios,
    validate_time_profile,
)

logger = logging.getLogger(__name__)

__all__ = ["ScenarioDataGenerator", "load_config"]


class ScenarioDataGenerator(BaseDataGenerator):
    """Generate synthetic tabular data with configurable data quality and time scenarios."""

    _FAKER_SKIP_KEYS: ClassVar[frozenset] = BaseDataGenerator._FAKER_SKIP_KEYS | frozenset(
        {"scenarios", "weighted_choice", "weights", "values"}
    )

    # Per-table state set by _pre_generate_table before the row loop
    _time_column: Optional[str] = None
    _time_values: List = []

    def _validate_config(self) -> None:
        super()._validate_config()
        tables_conf = self.config.get("tables", {})
        for tname, tconf in tables_conf.items():
            fields = tconf.get("fields", {})
            for fname, fconf in fields.items():
                if isinstance(fconf, dict):
                    scenarios = fconf.get("scenarios", {})
                    if scenarios:
                        if not isinstance(scenarios, dict):
                            raise ValueError(
                                f"Field 'scenarios' for {tname}.{fname} must be a mapping, "
                                f"got {type(scenarios).__name__}"
                            )
                        validate_field_scenarios(tname, fname, scenarios)

            time_profile = tconf.get("time_profile")
            if time_profile is not None:
                validate_time_profile(tname, time_profile)

            table_scenarios = tconf.get("scenarios")
            if table_scenarios is not None:
                if not isinstance(table_scenarios, dict):
                    raise ValueError(f"Table '{tname}' scenarios must be an object")
                if table_scenarios:
                    validate_table_scenarios(tname, table_scenarios)

    def _generate_field(self, fconf: Any, row_idx: int, current_row: Dict[str, Any]) -> Any:
        if isinstance(fconf, dict):
            if "weighted_choice" in fconf:
                wc = fconf["weighted_choice"]
                if not isinstance(wc, dict):
                    raise ValueError(
                        "weighted_choice must be an object with 'values' and 'weights'"
                    )
                value = choose_weighted_value(wc.get("values", []), wc.get("weights", []))
                return apply_field_scenarios(value, fconf)

            if "values" in fconf and "weights" in fconf:
                value = choose_weighted_value(fconf["values"], fconf["weights"])
                return apply_field_scenarios(value, fconf)

            # Delegate all base types to parent; wrap result with scenario application
            value = super()._generate_field(fconf, row_idx, current_row)
            return apply_field_scenarios(value, fconf)

        return fconf  # literal fallback

    def _pre_generate_table(self, tname: str, tconf: Dict[str, Any], count: int) -> None:
        time_profile = tconf.get("time_profile")
        if time_profile is not None:
            self._time_column = str(time_profile["column"])
            self._time_values = generate_weighted_timestamps(count, time_profile)
        else:
            self._time_column = None
            self._time_values = []

    def _generate_row_extras(self, row_idx: int, tconf: Dict[str, Any]) -> Dict[str, Any]:
        if self._time_column is not None:
            return {self._time_column: self._time_values[row_idx]}
        return {}

    def _post_generate_table(
        self, tname: str, tconf: Dict[str, Any], df: pd.DataFrame
    ) -> pd.DataFrame:
        return apply_table_scenarios(tname, tconf, df)

    def to_json(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        for tname, df in self.tables.items():
            path = os.path.join(out_dir, f"{tname}.json")
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(df.to_json(orient="records", indent=2, date_format="iso"))
            except OSError as exc:
                raise OSError(
                    f"Failed to write JSON output for table '{tname}' to '{path}'"
                ) from exc
