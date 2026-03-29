import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datafaker.generator import DataGenerator, load_config

cfg = load_config("examples/config.yaml")
print("Loaded config tables:", list(cfg.get("tables", {}).keys()))

g = DataGenerator(cfg, seed=42)
g.generate()
for tn, df in g.tables.items():
    print(tn, df.shape)

g.to_csv("out")
print("Wrote CSV to out/")
