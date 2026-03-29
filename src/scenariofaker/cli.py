from fakercore.cli_base import run_cli
from .generator import ScenarioDataGenerator


def main(argv=None) -> None:
    run_cli(
        ScenarioDataGenerator,
        "YAML-driven synthetic data generator with scenario controls",
        argv,
    )


if __name__ == "__main__":
    main()
