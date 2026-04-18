# rustwx

`rustwx` is a Rust-first weather workspace for model ingest, diagnostics, map rendering, sounding rendering, and thin optional Python bindings.

The design target is straightforward:

- keep the hot path in Rust
- support multiple gridded models behind one internal field model
- verify the science against existing reference implementations
- make high-volume plotting practical for operational use

This repo is early, but it is no longer a placeholder. The workspace compiles, tests, fetches live model data, renders real proof plots, and has a working HRRR ECAPE path plus cross-model upper-air proofs.

## Current scope

Built-in model/source coverage:

- `HRRR`
- `GFS`
- `ECMWF open data`
- `RRFS-A`

Working proof lanes:

- selector-backed upper-air map plots
- cross-model derived batch rendering for `HRRR`, `GFS`, `ECMWF open data`, and `RRFS-A`
- cross-model ECAPE 8-panel rendering for `HRRR`, `GFS`, `ECMWF open data`, and `RRFS-A`
- HRRR severe proof panel rendering
- sounding rendering through `sharprs` with external ECAPE annotations
- native composite reflectivity + UH proofs for HRRR and RRFS-A

Current design constraints:

- Rust-first compute and render path
- Python optional, never the main execution path
- model adapters should feed common field/selector types
- proof artifacts should be reproducible from code in this repo

## Workspace layout

| Crate | Role | Status |
| --- | --- | --- |
| `rustwx-core` | shared domain types and field/selector contracts | usable |
| `rustwx-models` | model registry, source planning, URL and recipe fetch planning | usable |
| `rustwx-io` | probing, byte-range fetch, selector-backed GRIB extraction, field cache | usable |
| `rustwx-prep` | preprocessing helpers, currently WRF-style lake masking/interpolation | narrow but real |
| `rustwx-calc` | ECAPE, CAPE/CIN, severe wrapper APIs over `metrust` | usable, still growing |
| `rustwx-render` | Rust PNG map renderer owned directly inside `rustwx` | usable |
| `rustwx-sounding` | sounding bridge and rendering over `sharprs` | usable |
| `rustwx-cli` | CLI and proof binaries | usable |
| `rustwx-python` | thin optional Python bindings | minimal |

Each crate has its own README in `crates/<crate>/README.md`.

## What works today

### Model and source plumbing

- model summaries and source priorities
- URL resolution for all four built-in models
- latest-run probing for NOAA-style feeds
- forecast-hour availability probes
- full-family HRRR ingest on the main operator path, with structured extraction
  from local `wrfsfc` / `wrfprs` family files
- full-file direct batching for `HRRR`, `GFS`, and `RRFS-A`, with one grouped
  structured-extraction pass feeding many direct plots per hour
- generic full-file derived batching for `HRRR`, `GFS`, `ECMWF open data`, and `RRFS-A`
- generic full-file ECAPE panel batching for `HRRR`, `GFS`, `ECMWF open data`, and `RRFS-A`
- indexed byte-range fetch remains available for models and product paths that
  are still configured for it
- on-disk artifact caching for fetched bytes and selector-backed field extraction

### Diagnostics

- ECAPE-family grid wrappers, including SB/ML/MU triplet support
- failure-mask variants for ECAPE debug/verification work
- CAPE/CIN wrappers
- SRH, shear, STP, SCP, EHI, local-proxy SHIP, and BRI wrappers
- bundled "supported severe" proof fields

### Rendering

- Solar07-backed filled maps
- contour overlays
- wind barb overlays
- panel composition
- sounding PNG rendering
- external ECAPE annotation block for soundings

## What does not work yet

- full generic model-to-product orchestration
- true all-model severe suite parity
- generic 3D ingest/decode APIs
- production-grade ECMWF latest-run probing
- full lake-aware preprocessing for non-WRF models
- GIF/animation orchestration in Rust
- typed Python API for render/fetch/calc workflows

## Vendored dependencies

This workspace no longer requires sibling local repos to build. The former
external crates are vendored under `vendor/`, and the checked-in basemap assets
needed by the render stack now live under `assets/basemap/`.

Imported upstream provenance is recorded in `vendor/VENDORED.md`.

## Quick start

### Build and test

```powershell
cargo test
```

### Basic CLI usage

```powershell
cargo run -p rustwx-cli -- list
cargo run -p rustwx-cli -- show hrrr
cargo run -p rustwx-cli -- latest hrrr 20260414
```

### Generate a selector-backed upper-air proof

```powershell
cargo run -p rustwx-cli --bin plot_recipe_proof -- `
  --model gfs `
  --date 20260414 `
  --hour 18 `
  --forecast-hour 0 `
  --recipe 500mb_temperature_height_winds `
  --region conus
```

### Generate the HRRR ECAPE proof panel

```powershell
cargo run -p rustwx-cli --release --bin hrrr_ecape8 -- `
  --date 20260414 `
  --hour 23 `
  --forecast-hour 0 `
  --region conus
```

### Generate one generic derived batch

```powershell
cargo run -p rustwx-cli --release --bin derived_batch -- `
  --model gfs `
  --all-supported `
  --date 20260414 `
  --cycle 18 `
  --forecast-hour 12 `
  --region midwest
```

### Generate one generic ECAPE 8-panel

```powershell
cargo run -p rustwx-cli --release --bin ecape8_batch -- `
  --model rrfs-a `
  --date 20260414 `
  --cycle 20 `
  --forecast-hour 2 `
  --source aws `
  --region midwest
```

### Generate one full-file direct batch for GFS or RRFS-A

```powershell
cargo run -p rustwx-cli --release --bin direct_batch -- `
  --model gfs `
  --all-supported `
  --date 20260414 `
  --cycle 18 `
  --forecast-hour 12 `
  --region midwest
```

```powershell
cargo run -p rustwx-cli --release --bin direct_batch -- `
  --model rrfs-a `
  --all-supported `
  --date 20260414 `
  --cycle 20 `
  --forecast-hour 2 `
  --source aws `
  --region midwest
```

### Generate one HRRR non-ECAPE hour pass

`hrrr_non_ecape_hour` is the main HRRR operator-facing batch entrypoint. It now
defaults to `NOMADS` and the full-family HRRR ingest path.

```powershell
cargo run -p rustwx-cli --release --bin hrrr_non_ecape_hour -- `
  --date 20260414 `
  --cycle 23 `
  --forecast-hour 1 `
  --region conus
```

## Proof artifacts

Representative proof outputs are committed under [proof/](proof/). The heavy fetch/decode caches are ignored.

Quantity semantics are explicit where the implementation is narrower than a generic meteorological label:

- the current vorticity proof path is absolute vorticity, not unspecified "vorticity"
- isobaric dewpoint extraction is currently direct DPT-only where wired, not a generic derived-dewpoint path
- the current SHIP wrapper is the existing local wrf-rust hail-proxy formula, not yet a full SHARPpy-style canonical SHIP implementation

Useful starting points:

- [HRRR ECAPE 8-panel](proof/rustwx_hrrr_20260414_23z_f00_conus_ecape8_panel.png)
- [HRRR severe proof panel](proof/rustwx_hrrr_20260414_23z_f00_midwest_severe_proof_panel.png)
- [GFS CONUS 500mb temperature / height / winds](proof/rustwx_gfs_20260414_18z_f000_conus_500mb_temperature_height_winds.png)
- [ECMWF CONUS 500mb temperature / height / winds](proof/rustwx_ecmwf_open_data_20260414_12z_f000_conus_500mb_temperature_height_winds.png)
- [HRRR Midwest composite reflectivity + UH](proof/rustwx_hrrr_20260414_23z_f000_midwest_composite_reflectivity_uh.png)
- [RRFS-A Midwest composite reflectivity + UH](proof/rustwx_rrfs_a_20260414_23z_f000_midwest_composite_reflectivity_uh.png)
- [Sounding proof with external ECAPE annotations](proof/rustwx_sounding_demo_external_ecape.png)

Timing JSON files live beside the PNGs.

## Performance notes

The fast path for upper-air plots is already heavily in Rust. On this machine, a cached Rust upper-air proof plot is sub-second, while a Python `matplotlib/cartopy` equivalent on the same cached GFS subset was about an order of magnitude slower.

The remaining expensive path is not upper-air rendering. It is full-grid severe/ECAPE computation, especially when solving SB/ML/MU parcel variants over large domains.

## Review guidance

If you are reviewing the workspace crate by crate, start in this order:

1. `rustwx-core`
2. `rustwx-models`
3. `rustwx-io`
4. `rustwx-calc`
5. `rustwx-render`
6. `rustwx-sounding`
7. `rustwx-prep`
8. `rustwx-cli`
9. `rustwx-python`

That order matches the dependency flow.

## Immediate next milestones

- broaden selector coverage and model adapters
- unify proof binaries into a real product CLI
- lift the generic direct/derived/ECAPE executor shape into windowed and severe products
- finish severe-suite render plumbing
- make ECMWF probing and fetch planning more robust
- reduce remaining ECAPE wall time
