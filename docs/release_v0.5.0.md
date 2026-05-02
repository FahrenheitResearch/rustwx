# rustwx v0.5.0 Release Notes

## Summary

v0.5.0 is the model-compatibility release. It keeps the Rust-first renderer and product paths, but broadens the supported model registry, adds local AIFS/Earth2Archive NetCDF support, adds generic ensemble-stat rendering surfaces, and proves indexed-GRIB windowed QPF across the easy operational forecast group.

This release does not claim every recipe works on every model. Availability remains selector/schema driven and is exposed through blockers in the product catalog and agent capabilities.

## Major Additions

- Added or expanded model IDs for `hrrr-ak`, `gdas`, `gefs`, `aigfs`, `aigefs`, `aifs`, `rap`, `nam`, `hiresw`, `sref`, `rtma`, `urma`, and `nbm`.
- Added local Earth2Archive NetCDF reading for AIFS/AIFSENS-style inference output under `$RUSTWX_EARTH2_ARCHIVE/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc`.
- Added AIFS latest-cycle and lead discovery for local Earth2 archives.
- Raised local AIFS lead support to multi-year inference runs, up to roughly five years.
- Added AIFS/Earth2 ensemble member/stat selectors for deterministic and ensemble-shaped NetCDF files.
- Added fast precomputed-stat paths for Earth2Archive aggregate variables such as `{var}_mean`, `{var}_std`, `{var}_p10`, `{var}_p50`, and `{var}_p90`.
- Added a generic GRIB ensemble reducer for one-file-per-member ensembles with mean/std/min/max/p10/p50/p90 and native-unit probability-of-exceedance outputs.
- Added indexed byte-range fetch defaults for NOAA/NCEP-style GRIB families that publish `.idx` sidecars.
- Added model-aware `qpf_total` windowed rendering across the validated indexed-GRIB forecast group.
- Exposed model-aware windowed products through the Python `render_maps_json()` path for Hermes and other agents.

## Product Coverage Snapshot

The non-ECAPE indexed-GRIB inventory currently reports these model/product combinations:

| Model | Direct | Derived | Windowed | Heavy | Total |
| --- | ---: | ---: | ---: | ---: | ---: |
| `hrrr` | 52 | 31 | 49 | 1 | 133 |
| `hrrr-ak` | 52 | 31 | 1 | 1 | 85 |
| `gfs` | 43 | 31 | 1 | 1 | 76 |
| `gdas` | 43 | 31 | 1 | 1 | 76 |
| `gefs` | 38 | 31 | 1 | 1 | 71 |
| `aigfs` | 33 | 31 | 1 | 1 | 66 |
| `aigefs` | 33 | 31 | 1 | 1 | 66 |
| `rap` | 33 | 31 | 1 | 1 | 66 |
| `nam` | 33 | 31 | 1 | 1 | 66 |
| `hiresw` | 33 | 31 | 1 | 1 | 66 |
| `sref` | 33 | 31 | 1 | 1 | 66 |
| `rrfs-a` | 49 | 31 | 1 | 1 | 82 |
| `nbm` | 16 | 31 | 1 | 1 | 49 |
| `rtma` | 16 | 31 | 0 | 1 | 48 |
| `urma` | 16 | 31 | 0 | 1 | 48 |

`aifs`, `ecmwf-open-data`, and `wrf-gdex` use separate whole-file/NetCDF paths and remain in the full product catalog.

## Validation Artifacts

Regenerate the catalog inventory:

```powershell
cargo run -q -p rustwx-cli --bin product_catalog -- --out target\model_compat_v05_inventory\product_catalog.json
python scripts\build_grib_map_inventory.py --catalog target\model_compat_v05_inventory\product_catalog.json --out-dir target\model_compat_v05_inventory
```

Windowed QPF proof artifacts:

```text
target/windowed_model_proof/windowed_qpf_contact_sheet.png
target/windowed_model_proof/WINDOWED_QPF_PROOF_SUMMARY.md
target/windowed_model_proof/windowed_qpf_proof_summary.csv
```

Model-compatibility approval contact sheet:

```text
target/model_compat_easy_group_approval/easy_group_contact_sheet.png
```

## Known Boundaries

- `qpf_total` is the only cross-model windowed product validated outside HRRR in this release. HRRR keeps the full QPF/UH/surface-extrema windowed family.
- `rtma` and `urma` are analysis grids and do not advertise forecast-window products.
- NBM renders and fetches through the catalog, but the current CONUS temperature visual still has a striping artifact and should remain visually flagged.
- ECMWF open-data GRIB remains a whole-file path. AIFS/Earth2Archive and WRF/GDEX are NetCDF paths.
- Generic JSON-defined composites and grid overlays are not implemented in `render_maps_json()` yet. Built-in recipes already support filled fields, isopleth/contour overlays, contour labels, wind overlays, and composite panels where the recipe defines them.

## Checks

Release-prep checks run locally:

```powershell
cargo test -q -p rustwx-products
cargo test -q -p rustwx-python --features python
cargo check -q --workspace
```

The GitHub release triggers the PyPI trusted-publishing workflow for wheels on Python 3.10 through 3.13 across Linux, Windows, and macOS.
