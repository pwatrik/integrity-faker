from datetime import datetime
import unittest

import pandas as pd

from scenariofaker.generator import ScenarioDataGenerator


class TestScenarioDataGenerator(unittest.TestCase):
    def test_missing_and_placeholders(self):
        config = {
            "tables": {
                "t": {
                    "count": 200,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "address": {
                            "faker": "street_address",
                            "scenarios": {"missing_probability": 0.25},
                        },
                        "external_id": {
                            "faker": "bothify",
                            "text": "ID-#####",
                            "scenarios": {
                                "placeholder_values": {
                                    "values": ["UNKNOWN", "MISSING"],
                                    "probability": 0.30,
                                }
                            },
                        },
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=7)
        tables = gen.generate()
        df = tables["t"]

        self.assertGreater(df["address"].isna().sum(), 0)
        placeholder_count = (df["external_id"].isin(["UNKNOWN", "MISSING"])) .sum()
        self.assertGreater(placeholder_count, 0)

    def test_outlier_scenario_creates_large_values(self):
        config = {
            "tables": {
                "orders": {
                    "count": 300,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "amount": {
                            "faker": "pyfloat",
                            "left_digits": 2,
                            "right_digits": 2,
                            "positive": True,
                            "min_value": 10,
                            "max_value": 100,
                            "scenarios": {
                                "outlier": {
                                    "probability": 0.10,
                                    "multiplier_min": 6,
                                    "multiplier_max": 10,
                                }
                            },
                        },
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=42)
        df = gen.generate()["orders"]
        self.assertGreater(df["amount"].max(), 500)

    def test_null_bursts_creates_contiguous_nulls(self):
        config = {
            "tables": {
                "events": {
                    "count": 120,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "address": {"faker": "street_address"},
                    },
                    "scenarios": {
                        "null_bursts": [
                            {
                                "columns": ["address"],
                                "burst_count": 2,
                                "burst_length_min": 10,
                                "burst_length_max": 10,
                            }
                        ]
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=1)
        df = gen.generate()["events"]
        is_null = df["address"].isna().astype(int)
        run_lengths = is_null.groupby((is_null != is_null.shift()).cumsum()).sum()
        self.assertTrue((run_lengths >= 10).any())

    def test_time_weighting_and_seasonal_burst(self):
        config = {
            "tables": {
                "events": {
                    "count": 3000,
                    "time_profile": {
                        "column": "event_ts",
                        "start": "2026-01-01T00:00:00",
                        "end": "2026-01-31T23:59:59",
                        "hour_weights": {
                            "2": 0.1,
                            "18": 4.0,
                        },
                        "seasonal_bursts": [
                            {
                                "start": "2026-01-10T00:00:00",
                                "end": "2026-01-12T23:59:59",
                                "multiplier": 8.0,
                            }
                        ],
                    },
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=101)
        df = gen.generate()["events"]
        event_ts = pd.to_datetime(df["event_ts"])

        at_18 = (event_ts.dt.hour == 18).sum()
        at_2 = (event_ts.dt.hour == 2).sum()
        self.assertGreater(at_18, at_2)

        in_burst = ((event_ts >= datetime.fromisoformat("2026-01-10T00:00:00")) &
                    (event_ts <= datetime.fromisoformat("2026-01-12T23:59:59"))).sum()
        outside_burst = len(event_ts) - in_burst
        self.assertGreater(in_burst / len(event_ts), 0.20)
        self.assertGreater(outside_burst, 0)

    def test_duplicate_key_bursts_create_contiguous_duplicates(self):
        config = {
            "tables": {
                "events": {
                    "count": 150,
                    "fields": {
                        "event_id": {"sequence": {"start": 1}},
                        "status": {"weighted_choice": {"values": ["ok", "fail"], "weights": [0.9, 0.1]}},
                    },
                    "scenarios": {
                        "duplicate_key_bursts": [
                            {
                                "column": "event_id",
                                "burst_count": 2,
                                "burst_length_min": 8,
                                "burst_length_max": 8,
                            }
                        ]
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=11)
        df = gen.generate()["events"]
        run_lengths = df["event_id"].groupby((df["event_id"] != df["event_id"].shift()).cumsum()).size()
        self.assertTrue((run_lengths >= 8).any())

    def test_late_arrivals_create_arrival_timestamp_later_than_event_timestamp(self):
        config = {
            "tables": {
                "events": {
                    "count": 400,
                    "time_profile": {
                        "column": "event_ts",
                        "start": "2026-01-01T00:00:00",
                        "end": "2026-01-15T23:59:59",
                    },
                    "fields": {
                        "event_id": {"sequence": {"start": 1}},
                    },
                    "scenarios": {
                        "late_arrivals": [
                            {
                                "event_time_column": "event_ts",
                                "arrival_time_column": "ingest_ts",
                                "probability": 0.30,
                                "min_delay_minutes": 120,
                                "max_delay_minutes": 1440,
                            }
                        ]
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=13)
        df = gen.generate()["events"]
        event_ts = pd.to_datetime(df["event_ts"])
        ingest_ts = pd.to_datetime(df["ingest_ts"])
        delayed_count = (ingest_ts > event_ts).sum()
        self.assertGreater(delayed_count, 0)

    def test_late_arrivals_defaults_arrival_column_to_ingest_ts(self):
        config = {
            "tables": {
                "events": {
                    "count": 400,
                    "time_profile": {
                        "column": "event_ts",
                        "start": "2026-01-01T00:00:00",
                        "end": "2026-01-15T23:59:59",
                    },
                    "fields": {
                        "event_id": {"sequence": {"start": 1}},
                    },
                    "scenarios": {
                        "late_arrivals": [
                            {
                                "event_time_column": "event_ts",
                                "probability": 0.30,
                                "min_delay_minutes": 120,
                                "max_delay_minutes": 1440,
                            }
                        ]
                    },
                }
            }
        }

        gen = ScenarioDataGenerator(config, seed=13)
        df = gen.generate()["events"]
        self.assertIn("ingest_ts", df.columns)
        event_ts = pd.to_datetime(df["event_ts"])
        ingest_ts = pd.to_datetime(df["ingest_ts"])
        # event timestamps must never be mutated; arrival must be >= event
        self.assertTrue((ingest_ts >= event_ts).all())
        delayed_count = (ingest_ts > event_ts).sum()
        self.assertGreater(delayed_count, 0)


if __name__ == "__main__":
    unittest.main()
