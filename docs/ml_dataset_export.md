# ML Dataset Export

`rustwx` now exposes a stable NPY-directory export contract for the Julia / diffusion stack while keeping the file format generic.

## Contract

The exporter writes:

- `dataset_manifest.json`
- `dataset_build_manifest.json`
- `samples/<sample_id>/sample_manifest.json`
- `samples/<sample_id>/<channel>.npy`

The compatibility surface intentionally preserves the fields the current Julia loader reads:

- `dataset_manifest.channels`
- `dataset_manifest.labels`
- `dataset_build_manifest.samples[*].relative_dir`
- `sample_manifest.channels[*].name`
- `sample_manifest.channels[*].data_file`

Everything else added by `rustwx` is extra metadata for provenance and future hybrid work.

## Shared Metadata

Per sample:

- `valid_time_utc`
- `cycle_init_utc`
- `forecast_hour`
- `model`
- `source`
- `split`
- `grid.shape`
- `grid.projection`
- `grid.grid_domain_id`
- `grid.approx_dx_km`
- `grid.approx_dy_km`

Per channel:

- `name`
- `canonical_name`
- `units`
- `shape`
- `level`
- `level_hpa`
- `height_m_agl`
- `kind`
- `experimental`
- `proxy`
- `compatibility_alias_of`
- `provenance.route`
- `provenance.product_identity`
- `provenance.field_selector`
- `provenance.input_fetch_keys`
- `provenance.resolved_fetch_urls`

The bundle format still does **not** encode ML roles like `input`, `target`, `state`, or `loss head`.

## Presets

### `mesoconvective_v1`

This is now an explicit compatibility preset for the current Julia workflow.

Channels:

- `t2m`
- `d2m`
- `q2m`
- `u10`
- `v10`
- `wind_speed`
- `wind_direction`
- `relative_humidity`
- `cape`
- `refc`

Important:

- `cape` is kept only as a compatibility alias.
- The channel metadata explicitly marks `cape` as `compatibility_alias_of = "sbcape"`.
- The dataset manifest also carries `compatibility_mode = "wxtrain_legacy_cape_alias"`.

So the old contract stays readable by Julia, but the CAPE semantics are no longer silent.

### `hybrid_column_v1`

This is the next serious export profile for hybrid Julia + diffusion work.

Surface/state:

- `t2m`
- `d2m`
- `q2m`
- `u10`
- `v10`
- `wind_speed`
- `wind_direction`
- `relative_humidity`
- `mslp`
- `terrain`
- `refc`

Pressure-level state:

- `t925`
- `t850`
- `t700`
- `rh925`
- `rh850`
- `rh700`
- `z925`
- `z850`
- `z700`
- `u925`
- `u850`
- `u700`
- `v925`
- `v850`
- `v700`

Severe ingredients:

- `sbcape`
- `sbcin`
- `mlcape`
- `mlcin`
- `mucape`
- `srh01`
- `srh03`
- `shear06`
- `sblcl`

Optional heavy derived set when verified:

- `sbecape`
- `mlecape`
- `muecape`

These can be disabled at export time with `--no-ecape`; the bundle manifests then record `excluded_optional_groups: ["ecape"]` so downstream Julia code can distinguish an intentional fast export from a truncated or broken sample.

Current honest gap:

- `w700` / `omega700` is intentionally **not** exported yet because there is not a verified public rustwx field path for it. The preset stays honest instead of inventing a proxy.

## Model Policy

### HRRR

`hybrid_column_v1` is supported.

The exporter reuses `rustwx` for:

- fetch and decode
- surface thermodynamics
- pressure-level state
- severe diagnostics
- ECAPE triplet fields
- direct reflectivity / MSLP field extraction

### RRFS-A

`hybrid_column_v1` is supported only through verified surface/pressure semantics and derived ingredients.

That means:

- decoded surface state is allowed
- decoded pressure-level state is allowed
- severe ingredients derived from decoded state are allowed
- ECAPE triplet fields derived from decoded state are allowed when explicitly enabled
- thermo-native candidate/proxy mappings are **not** used by this exporter
- direct-field channels `mslp` and `refc` are currently **excluded** from RRFS-A `hybrid_column_v1` until those public rustwx mappings are verified end to end

### WRF / WRF-GDEX

Not implemented yet.

The preset and manifest system are designed so a future NetCDF / WRF adapter can plug in without changing the NPY bundle contract.

## Examples

Mesoconvective compatibility export:

```powershell
cargo run -p rustwx-cli --bin hrrr_dataset_export -- `
  --model hrrr `
  --preset mesoconvective-v1 `
  --dataset-name rustwx_hrrr_mesoconvective_demo `
  --date 20260422 `
  --cycle 7 `
  --forecast-hour 0 `
  --source nomads `
  --split train `
  --out-dir target\hrrr_dataset_export_demo
```

Hybrid HRRR export:

```powershell
cargo run -p rustwx-cli --bin hrrr_dataset_export -- `
  --model hrrr `
  --preset hybrid-column-v1 `
  --dataset-name rustwx_hrrr_hybrid_column_demo `
  --date 20260422 `
  --cycle 7 `
  --forecast-hour 0 `
  --source nomads `
  --split train `
  --out-dir target\hrrr_hybrid_column_demo
```

Hybrid HRRR export without optional ECAPE channels:

```powershell
cargo run -p rustwx-cli --bin hrrr_dataset_export -- `
  --model hrrr `
  --preset hybrid-column-v1 `
  --no-ecape `
  --dataset-name rustwx_hrrr_hybrid_column_fast_demo `
  --date 20260422 `
  --cycle 7 `
  --forecast-hour 0 `
  --source nomads `
  --split train `
  --out-dir target\hrrr_hybrid_column_fast_demo
```

Hybrid RRFS-A export:

```powershell
cargo run -p rustwx-cli --bin hrrr_dataset_export -- `
  --model rrfs-a `
  --preset hybrid-column-v1 `
  --region reno-square `
  --no-ecape `
  --dataset-name rustwx_rrfs_hybrid_column_demo `
  --date 20260422 `
  --cycle 7 `
  --forecast-hour 0 `
  --source aws `
  --split validation `
  --out-dir target\rrfs_hybrid_column_demo
```

## Julia Training Note

This exporter supports hybrid training cleanly, but does not force training policy into the file format.

Reasonable downstream usage buckets:

Primary state supervision:

- `t2m`
- `d2m`
- `q2m`
- `u10`
- `v10`
- `terrain`
- `t925`
- `t850`
- `t700`
- `rh925`
- `rh850`
- `rh700`
- `z925`
- `z850`
- `z700`
- `u925`
- `u850`
- `u700`
- `v925`
- `v850`
- `v700`

Note: RRFS-A currently exports the same thermodynamic/kinematic state channels except for the unverified direct-field channels `mslp` and `refc`.

Auxiliary ingredient heads:

- `sbcape`
- `sbcin`
- `mlcape`
- `mlcin`
- `mucape`
- `srh01`
- `srh03`
- `shear06`
- `sblcl`
- `sbecape`
- `mlecape`
- `muecape`

Diagnostic-only or presentation-heavy fields:

- `refc`
- `wind_speed`
- `wind_direction`
- `relative_humidity`

That split belongs in Julia / the training repo, not in the bundle format.

## Migration Note

For HRRR, once this exporter is adopted, the following `wxtrain` Rust path can be retired or reduced to compatibility wrappers:

- `crates/wx-cli/src/feature_materialize.rs`
  - HRRR fetch/decode/materialization
  - mesoconvective channel derivation
- `crates/wx-export/src/lib.rs`
  - HRRR NPY-directory writer and manifest path

What stays downstream for now:

- Julia-side dataset consumption
- ML role assignment
- loss selection
- job/spec orchestration
