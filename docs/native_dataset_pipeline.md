# Rust-Native Multisource Dataset Pipeline

This pipeline materializes hour-major training shards from HRRR, GOES ABI, MRMS, and NEXRAD Level-II sources.

The main path is:

```text
native_dataset_plan -> native_dataset_runner --shard-out -> native_dataset_shard_export
```

## Plan

Create a plan with explicit cases, tiles, and grid size:

```powershell
cargo run -p rustwx-cli --bin native_dataset_plan -- `
  --dataset-name hrrr_multisource_v1 `
  --case 20240506_ok_ks,2024-05-06T18:00:00Z,4 `
  --tile-grid -100,-99,35,36,1,1 `
  --grid-size 512 `
  --out target\native_dataset_plan\dataset_plan.json
```

The default source set is:

```text
HRRR wrfsfc: t2m, d2m, u10, v10, cape, cin, refc, mslp, terrain, pwat
MRMS: refc, llz, prate
GOES ABI MCMIPC: C01, C02, C03, C07, C08, C09, C10, C13
NEXRAD Level-II: reflectivity, velocity
```

For wider satellite/radar tensors, `native_dataset_plan` accepts presets:

```powershell
  --goes-channels C01-C16 `
  --goes-derived all `
  --level2-products all
```

`--goes-channels` also accepts `core`, `all`, and ranges such as `C07-C16`.
`--goes-derived` accepts `all` for ABI-derived fields such as brightness
temperature differences and normalized visible-channel differences.
`--level2-products` accepts `core` and `all`.

`terrain` is currently carried in the schema and filled as missing until the terrain selector is mapped.

## Materialize

Use a local raw cache when available:

```powershell
cargo run -p rustwx-cli --bin native_dataset_runner -- `
  --plan target\native_dataset_plan\dataset_plan.json `
  --source-root D:\wx_raw `
  --shard-out target\native_dataset_shards\shard_00000 `
  --cache-root target\native_dataset_cache `
  --progress-out target\native_dataset_shards\progress.jsonl `
  --report-out target\native_dataset_shards\report.json
```

Expected raw-cache layout:

```text
raw/
  hrrr/hrrr_YYYYMMDD_HH.grib2
  goes/OR_ABI-L2-MCMIPC-*.nc
  mrms/refc/MRMS_MergedReflectivityQCComposite_00.50_YYYYMMDD-HHMMSS.grib2[.gz]
  mrms/llz/MRMS_MergedReflectivityQC_00.50_YYYYMMDD-HHMMSS.grib2[.gz]
  mrms/prate/MRMS_PrecipRate_00.00_YYYYMMDD-HHMMSS.grib2[.gz]
  level2/KTLX/KTLXYYYYMMDD_HHMMSS_V06
```

Network fetch can be enabled per family:

```powershell
  --fetch-hrrr   # rustwx model fetch/cache path
  --fetch-obs    # public GOES-16 and MRMS S3 mirrors
  --fetch-radar  # public NEXRAD Level-II S3 mirror
```

`--allow-missing-sources` fills a missing or unreadable source family with NaNs. Use it for profiling and partial-cache tests, not final training data.

## Time Windows

Local nearest-file matching is bounded:

```text
GOES: +/- 30 minutes
MRMS: +/- 20 minutes
NEXRAD Level-II: +/- 10 minutes
```

This prevents stale observation files from silently satisfying later hours.

## Output

The runner writes a raw f32 shard:

```text
hrrr_f32.bin
mrms_f32.bin
goes_f32.bin
radar_f32.bin
target_f32.bin
index.jsonl
manifest.json
```

The tensor layout is recorded in `manifest.json`. Current target tensors include MRMS reflectivity/rate, GOES C13, reflectivity initiation, and current MRMS reflectivity.
