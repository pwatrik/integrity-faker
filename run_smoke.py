from datafaker.generator import load_config, DataGenerator

cfg = load_config('examples/config.yaml')
print('Loaded config tables:', list(cfg.get('tables', {}).keys()))

g = DataGenerator(cfg, seed=42)
g.generate()
for tn, df in g.tables.items():
    print(tn, df.shape)

# write to temp duckdb\ng.to_csv('out')
print('Wrote CSV to out/')
