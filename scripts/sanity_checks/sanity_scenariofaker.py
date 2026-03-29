import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scenariofaker.generator import ScenarioDataGenerator, load_config

cfg = load_config("examples/scenario_config.yaml")
print("Loaded config tables:", list(cfg.get("tables", {}).keys()))

g = ScenarioDataGenerator(cfg, seed=42)
g.generate()
for tn, df in g.tables.items():
    print(tn, df.shape)

g.to_csv("out/scenario")
print("Wrote CSV to out/scenario/")
