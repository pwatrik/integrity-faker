import unittest

from datafaker.generator import DataGenerator


class TestDataGenerator(unittest.TestCase):
    def test_referential_integrity_generation(self):
        config = {
            "tables": {
                "patients": {
                    "count": 10,
                    "fields": {
                        "patient_id": {"sequence": {"start": 1}},
                        "name": {"faker": "name"},
                    },
                },
                "encounters": {
                    "count": 50,
                    "fields": {
                        "encounter_id": {"sequence": {"start": 1}},
                        "patient_id": {"fk": "patients.patient_id"},
                    },
                },
            }
        }

        generator = DataGenerator(config, seed=123)
        tables = generator.generate()

        self.assertEqual(len(tables["patients"]), 10)
        self.assertEqual(len(tables["encounters"]), 50)

        patient_ids = set(tables["patients"]["patient_id"].tolist())
        encounter_patient_ids = set(tables["encounters"]["patient_id"].tolist())
        self.assertTrue(encounter_patient_ids.issubset(patient_ids))

    def test_validate_config_rejects_unknown_fk_table(self):
        config = {
            "tables": {
                "appointments": {
                    "count": 1,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "patient_id": {"fk": "patients.patient_id"},
                    },
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "unknown table"):
            DataGenerator(config)

    def test_validate_config_rejects_unknown_fk_field(self):
        config = {
            "tables": {
                "patients": {
                    "count": 1,
                    "fields": {
                        "patient_id": {"sequence": {"start": 1}},
                    },
                },
                "appointments": {
                    "count": 1,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "patient_ref": {"fk": "patients.nonexistent_field"},
                    },
                },
            }
        }

        with self.assertRaisesRegex(ValueError, "unknown field"):
            DataGenerator(config)

    def test_faker_provider_accepts_arguments(self):
        config = {
            "tables": {
                "people": {
                    "count": 25,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "age": {"faker": "random_int", "min": 18, "max": 65},
                    },
                }
            }
        }

        generator = DataGenerator(config, seed=123)
        tables = generator.generate()

        self.assertEqual(len(tables["people"]), 25)
        ages = tables["people"]["age"].tolist()
        self.assertTrue(all(18 <= age <= 65 for age in ages))

    def test_faker_provider_rejects_unknown_provider(self):
        config = {
            "tables": {
                "items": {
                    "count": 1,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "value": {"faker": "definitely_not_a_provider"},
                    },
                }
            }
        }

        generator = DataGenerator(config)
        with self.assertRaisesRegex(ValueError, "Unknown faker provider"):
            generator.generate()

    def test_faker_provider_rejects_invalid_arguments(self):
        config = {
            "tables": {
                "people": {
                    "count": 1,
                    "fields": {
                        "id": {"sequence": {"start": 1}},
                        "age": {"faker": "random_int", "minimum": 1, "maximum": 10},
                    },
                }
            }
        }

        generator = DataGenerator(config)
        with self.assertRaisesRegex(ValueError, "Invalid arguments for faker provider"):
            generator.generate()


if __name__ == "__main__":
    unittest.main()
