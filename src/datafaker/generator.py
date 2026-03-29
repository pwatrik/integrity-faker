from fakercore.base import BaseDataGenerator, load_config

__all__ = ["DataGenerator", "load_config"]


class DataGenerator(BaseDataGenerator):
    """Generate synthetic tabular data from YAML config with FK integrity."""
