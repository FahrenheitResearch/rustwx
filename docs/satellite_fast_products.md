# Satellite Fast Products

rustwx now has three satellite-oriented paths that share the same ABI/netcrust
reader stack.

## Native Crops and Loops

`goes_native_sequence` discovers GOES ABI files, caches source NetCDF files, and
renders fixed-grid crops or loops for arbitrary geographic bounds.

The crop renderer uses NetCDF/HDF5 hyperslab reads for the ABI `CMI` variables,
so regional products do not need to load the full ABI scene into memory.

Example:

```powershell
cargo run --release -p rustwx-cli --bin goes_native_sequence -- `
  --satellite goes18 `
  --sector conus `
  --product geocolor `
  --west=-126.3 --east=-119.0 --south=43.0 --north=47.8 `
  --start 2026-05-08T13:00:00Z `
  --end 2026-05-09T03:30:00Z `
  --min-step-minutes 10 `
  --out-dir target\goes_portland_loop `
  --cache-dir target\goes_cache
```

Supported products include single bands (`band_13`, `C02`) and RGB recipes such
as `geocolor`, `airmass`, `dust`, `fire_temperature`, `sandwich`, and
`day_night_cloud_micro_combo`.

## Web Overlay Tiles

`png_web_tiles` cuts a rendered static product into transparent XYZ PNG tiles and
writes a `tilejson.json` manifest usable by web map clients.

Example:

```powershell
cargo run --release -p rustwx-cli --bin png_web_tiles -- `
  --input-png target\goes_portland_loop\frame.png `
  --out-dir target\goes_portland_tiles `
  --name "Portland GOES GeoColor" `
  --west=-126.3 --east=-119.0 --south=43.0 --north=47.8 `
  --min-zoom 5 `
  --max-zoom 7
```

The tile cutter is intentionally generic: it can be used for satellite crops,
model static plots, MRMS plots, or other already-rendered transparent products.

## ML Training Data

The native dataset pipeline can build hour-major training shards from:

- HRRR fields
- GOES ABI channels
- MRMS products
- NEXRAD Level-II products

Use `native_dataset_plan` presets for wider satellite/radar tensors:

```powershell
cargo run -p rustwx-cli --bin native_dataset_plan -- `
  --dataset-name hrrr_goes_mrms_level2_v1 `
  --case 20260508_portland,2026-05-08T21:00:00Z,6 `
  --tile-grid=-126.3,-119.0,43.0,47.8,1,1 `
  --grid-size 512 `
  --goes-sector conus `
  --goes-channels C01-C16 `
  --goes-derived all `
  --level2-products all `
  --out target\native_dataset_plan\dataset_plan.json
```

`--goes-channels` supports `core`, `all`, or ranges such as `C01-C16`.
`--goes-derived` supports `all` for ABI difference channels such as
`btd_c13_c15`, `btd_c08_c10`, and `ndiff_c02_c01`; the materializer loads the
raw source channels and computes those tensors during shard creation.
`--level2-products` supports `core` or `all`.
