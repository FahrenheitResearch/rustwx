# rustwx Model Compatibility v0.5

This document is the handoff for adding model support without inventing a one-off path per model.

## Goal

Every forecast source should enter rustwx through the same layers:

1. `rustwx-core`: stable model/source IDs and canonical selectors.
2. `rustwx-models`: cycle cadence, horizons, source URLs, capability metadata, and recipe gating.
3. `rustwx-io`: fetch/local-open plus field extraction into `SelectedField2D`.
4. `rustwx-products`: direct/derived/render products consume canonical selectors and do not know the source format.

If a new model can satisfy canonical selectors, it should render through existing direct and derived product paths.

## Supported Runtime Families

| Runtime family | Existing examples | Add-model work |
| --- | --- | --- |
| `grib2_forecast` | HRRR, GFS, GEFS, ECMWF IFS, ECMWF AIFS-Single, RRFS-A | Register source URLs and GRIB selector support. Prefer `.idx` range fetch when available. |
| `local_netcdf_forecast` | Earth2Archive AIFS/AIFSENS NetCDF | Register a local archive URL and add a NetCDF selector adapter in `rustwx-io`. |
| `wrf_netcdf_archive` | WRF/GDEX | Register archive URL grammar and a NetCDF selector adapter. |

## Current v0.5 Model Targets

| Model ID | Source path | Status |
| --- | --- | --- |
| `hrrr` | NOAA GRIB2 | Existing operational path. |
| `hrrr-ak` | NOAA HRRR Alaska GRIB2 | Added as the Alaska HRRR surface/pressure/native family. |
| `gfs` | NOAA GRIB2 | Existing global path. |
| `gdas` | NOAA GDAS 0.25 degree GRIB2 | Added for GDAS analysis/short forecast grids. |
| `gefs` | NOAA GEFS GRIB2 | Added as member/stat-file ensemble support. Product names such as `pgrb2ap5/gec00`, `pgrb2bp5/gec00`, and `pgrb2sp25/geavg` select products. |
| `aigfs` | NOAA AIGFS GRIB2 | Added for operational AI-GFS surface and pressure grids. |
| `aigefs` | NOAA AIGEFS GRIB2 | Added for operational AI-GEFS ensemble-stat surface and pressure grids. |
| `ecmwf-open-data` | ECMWF IFS Open Data GRIB2 | Existing open-data path. |
| `aifs` | `Earth2Archive` local NetCDF and ECMWF AIFS-Single GRIB2 | Added. Local archive supports experimental long leads; ECMWF open data follows ECMWF step cadence. |
| `rap` | NOAA RAP GRIB2 | Added for CONUS/North America RAP products. |
| `nam` | NOAA NAM GRIB2 | Added for parent and nest product families; pressure plots usually require `awip3d`. |
| `hiresw` | NOAA HIRESW GRIB2 | Added for ARW/FV3 high-resolution windows. |
| `sref` | NOAA SREF GRIB2 | Added for member/stat products. |
| `rtma` | NOAA RTMA GRIB2 | Added as surface-analysis grids only. |
| `urma` | NOAA URMA GRIB2 | Added as surface-analysis grids only. |
| `nbm` | NOAA National Blend GRIB2 | Added as core forecast grids; visual QA is still flagged for the current CONUS temperature render. |
| `rrfs-a` | NOAA RRFS-A GRIB2 | Existing regional path. |
| `wrf-gdex` | UCAR GDEX WRF NetCDF | Existing archive path. |

Not yet included: `href`. Its current public NOMADS/AWS layout was not verified during this slice, so it remains a follow-up rather than a guessed URL grammar.

## Earth2Archive Layout

Set:

```text
RUSTWX_EARTH2_ARCHIVE=C:\path\to\earth2_archive
```

Expected layout:

```text
$RUSTWX_EARTH2_ARCHIVE/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc
```

Example:

```text
C:\wxdata\aifs-earth2-archive\aifs\20160822T00Z\lead024.nc
```

Long inference runs can use more than three lead digits:

```text
earth2-archive://aifs/20260502T00Z/lead43848.nc
```

The current AIFS NetCDF adapter maps these variables:

| Canonical selector | Earth2 variable |
| --- | --- |
| 2m temperature | `t2m` |
| 2m dewpoint | `d2m` |
| 2m relative humidity | derived from `t2m` + `d2m` |
| 10m wind | `u10m`, `v10m` |
| surface pressure | `sp` |
| MSLP | `msl` |
| PWAT/TCW | `tcw` |
| total cloud cover | `tcc`, converted to percent when stored as 0-1 |
| total precipitation | `tp06`, converted from meters to mm |
| pressure temperature/wind | `t{hpa}`, `u{hpa}`, `v{hpa}` |
| pressure height | `z{hpa}` converted from geopotential to meters |
| pressure dewpoint/RH | derived from `q{hpa}` |

The adapter intentionally blocks fields not present in that schema, such as absolute vorticity, gust, visibility, and low/mid/high cloud cover.

## GEFS Member Selection

GEFS uses one GRIB2 file per member/stat product. Use the native product token to select the member:

```text
pgrb2ap5/gec00
pgrb2ap5/gep01
pgrb2ap5/gep30
pgrb2ap5/geavg
pgrb2ap5/gespr
pgrb2bp5/gec00
pgrb2sp25/geavg
```

Examples:

```powershell
cargo run -p rustwx-cli -- url gefs 20260502 0 24 pgrb2ap5/gep03
cargo run -p rustwx-cli --bin direct_batch -- --model gefs --source aws --date 20260502 --cycle 0 --forecast-hour 24 --region conus --recipe 2m_temperature_10m_winds
```

## Generic GRIB Ensemble Reducer

`grib_ensemble_reduce` reduces member GRIB files through the same direct recipe renderer used by deterministic maps. It is not GEFS-only; GEFS just has a default member list (`pgrb2ap5/gec00` plus `pgrb2ap5/gep01` through `pgrb2ap5/gep30`). Other one-file-per-member GRIB ensembles can use explicit `--member-product` values or a `--member-template`.

Supported statistics:

```text
mean, std, min, max, p10, p50, p90, prob-exceed
```

Mean/min/max/percentile products reuse the recipe's normal palette and overlays. Spread and probability maps use dedicated sequential palettes and intentionally skip vector/contour overlays because those statistics are not physical wind/height fields.

Examples:

```powershell
cargo run -p rustwx-cli --bin grib_ensemble_reduce -- --model gefs --source aws --date 20260502 --cycle 0 --forecast-hour 24 --region conus --recipe 2m_temperature_10m_winds --stat mean
cargo run -p rustwx-cli --bin grib_ensemble_reduce -- --model gefs --source aws --date 20260502 --cycle 0 --forecast-hour 24 --region conus --recipe 2m_temperature_10m_winds --stat std
cargo run -p rustwx-cli --bin grib_ensemble_reduce -- --model gefs --source aws --date 20260502 --cycle 0 --forecast-hour 24 --region conus --recipe 2m_temperature --stat prob-exceed --threshold 300 --threshold-op gt
```

Probability thresholds are in the native units of the selected GRIB field. For `2m_temperature`, that means Kelvin.

Validation artifacts from the initial GEFS smoke are here:

```text
target/grib_ensemble_reduce_smoke/gefs_ensemble_reduce_contact_sheet.png
```

## Indexed GRIB Fetch Contract

Direct-map GRIB products now prefer `.idx` byte-range fetches for the NOAA/NCEP-style model families where the source publishes sidecars. This applies to HRRR, HRRR-AK, GFS, GDAS, GEFS, AIGFS, AIGEFS, RAP, NAM, HIRESW, SREF, RTMA, URMA, NBM, and RRFS-A.

ECMWF open data is still a whole-file GRIB exception because the current adapter does not have a NOAA-style `.idx` source. Earth2Archive AIFS/AIFSENS is NetCDF and uses its own local-file path.

## Windowed GRIB Proof

`hrrr_windowed_batch` is now model-aware despite the legacy binary name. The release proof generated short-window `qpf-total` maps for every registered NOAA/NCEP-style idx-capable forecast GRIB model in the easy group:

```text
hrrr f002, hrrr-ak f002, gfs f002, gdas f002, gefs f003,
aigfs f006, aigefs f006, rap f002, nam f002, hiresw f002,
sref f003, nbm f002, rrfs-a f002
```

The proof artifacts are here:

```text
target/windowed_model_proof/windowed_qpf_contact_sheet.png
target/windowed_model_proof/WINDOWED_QPF_PROOF_SUMMARY.md
target/windowed_model_proof/windowed_qpf_proof_summary.csv
```

Scope notes:

- `rtma` and `urma` are analysis-only grids, so they are not valid short-window QPF proof targets.
- ECMWF open-data GRIB remains a whole-file non-idx path, and Earth2Archive AIFS/AIFSENS plus WRF/GDEX are NetCDF paths. Those need separate windowed support decisions if accumulation-window products are required there.
- NBM APCP currently needs a fallback that treats the F-hour APCP field as an F-hour accumulation when parsed GRIB time-range metadata is absent. The report keeps that run separate in the proof table.

Release-review inventory files:

```text
target/model_compat_v05_inventory/GRIB_MAP_INVENTORY_NON_ECAPE.md
target/model_compat_v05_inventory/grib_map_inventory_non_ecape.csv
```

Regenerate with:

```powershell
cargo run -q -p rustwx-cli --bin product_catalog -- --out target\model_compat_v05_inventory\product_catalog.json
python scripts\build_grib_map_inventory.py --catalog target\model_compat_v05_inventory\product_catalog.json --out-dir target\model_compat_v05_inventory
```

## Easy NCEP GRIB Group Added In This Slice

These models now have registered IDs, aliases, forecast-hour rules, URL builders, probe coverage, recipe fetch defaults, and conservative recipe blockers:

| Model | Example product | Notes |
| --- | --- | --- |
| `hrrr-ak` | `sfc`, `prs`, `nat` | Alaska HRRR file naming and source paths. |
| `gfs` variants | `pgrb2.0p25`, `pgrb2.0p50`, `pgrb2.1p00`, `pgrb2b.0p25`, `sflux` | Existing model ID with broader product-token support. |
| `gdas` | `pgrb2.0p25` | Analysis and short forecast products. |
| `gefs` variants | `pgrb2ap5`, `pgrb2bp5`, `pgrb2sp25` | Member/stat token remains part of the product string. |
| `aigfs` | `sfc`, `pres` | AI-GFS surface and pressure grids. |
| `aigefs` | `sfc/avg`, `pres/avg` | AI-GEFS stat products. |
| `rap` | `awp130pgrb` | RAP pressure/surface products. |
| `nam` | `awip12`, `awip3d`, nests | Parent and nest URL grammar; 3D pressure plots need a pressure-capable product. |
| `hiresw` | `arw_2p5km/conus` | ARW/FV3 high-resolution windows. |
| `sref` | `arw/ctl/pgrb132`, `ensprod/pgrb212/mean_3hrly` | Member and ensemble-stat layouts. |
| `rtma`, `urma` | `2dvaranl_ndfd` | Surface-analysis grids only. |
| `nbm` | `core/co` | Core blend forecasts; current direct render has a visible striping artifact and should stay visually flagged. |

Approval plots for this slice were generated under:

```text
target/model_compat_easy_group_approval/easy_group_contact_sheet.png
```

The probes passed against current public data for the easy group, but release approval should still be visual. In particular, the NBM core-grid render is functional but not yet visually clean enough to call fully production-grade.

## Adding The Next Model

1. Add `ModelId` aliases in `crates/rustwx-core/src/lib.rs`.
2. Add source IDs only if the source class is new.
3. Add a `ModelSummary` in `crates/rustwx-models/src/lib.rs` with `runtime_family` and `ensemble_mode`.
4. Add URL builder logic and tests for at least one real current URL.
5. Add selector support and blockers. Do not advertise a recipe just because another model has the field.
6. If the source is not GRIB2, add one `rustwx-io` adapter that returns `SelectedField2D`.
7. Wire product fetch defaults in `plot_recipe_fetch_defaults` and bundle fetch patterns if `.idx` subsetting applies.
8. Add smoke tests:
   - URL resolves to the public/current layout.
   - source probe succeeds when data exist.
   - one direct product renders.
   - optional local fixture extraction test skips cleanly when local data are absent.

## Release Boundaries

v0.5 should claim broad compatibility scaffolding and working paths for the models above. It should not claim every possible recipe works on every model. Product availability is still selector/schema dependent and must remain explicit in blockers and capabilities.
