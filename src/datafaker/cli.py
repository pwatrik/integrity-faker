from fakercore.cli_base import run_cli
from .generator import DataGenerator


def main(argv=None) -> None:
    run_cli(
        DataGenerator,
        "YAML-driven synthetic tabular data generator with referential integrity",
        argv,
    )


if __name__ == "__main__":
    main()
