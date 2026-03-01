# US State-to-State Migration Map

Interactive visualization of annual U.S. state migration corridors.

## Included Files

- `migration_flow_3d.html`: interactive map UI
- `state_to_state_migration_normalized.csv`: normalized migration corridor dataset
- `build_combined_dataset.py`: source table aggregation script
- `build_normalized_state_flows.py`: normalization/build script
- `download_manifest.csv`: source file manifest

## Run Locally

From this folder:

```powershell
python -m http.server 8787
```

Open:

`http://127.0.0.1:8787/migration_flow_3d.html`

## Data Source

Derived from U.S. Census state-to-state migration tables listed in `download_manifest.csv`.
