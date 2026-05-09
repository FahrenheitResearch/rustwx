# rustwx v0.5.7

v0.5.7 adds a fast GOES ABI sequence path for regional satellite imagery and
agent workflows.

## Changes

- Added `goes_native_sequence` for native-grid GOES crops over arbitrary
  latitude/longitude bounds and explicit time windows.
- Added NetCDF/HDF5 window reads for ABI `CMI` variables, so regional crops do
  not need to decode full CONUS or full-disk scenes.
- Added `png_web_tiles` for transparent XYZ tile overlays from rendered PNG
  products.
- Added `render_goes_native_sequence_json` to the Python package.
- Expanded native training-data planning for full GOES channel sets,
  derived GOES fields, and wider Level-II product sets.

## Validation

- Southern Plains GOES-19 GeoColor loop, 56 frames at 1600x900:
  - cold cache: 431.62 s wall time, 417.9 s download, 11.7 s render
  - warm cache: 14.58 s wall time, 12.8 s render
- Portland/Pacific Northwest GOES-18 GeoColor loop, 87 frames:
  - warm cache: 13.9 s wall time

## Checks

```text
cargo check -p rustwx-python --features python
cargo check -p rustwx-cli --bin goes_native_sequence --bin png_web_tiles --bin native_dataset_plan
cargo test -p rustwx-products native_dataset_materializer::tests
cargo test -p rustwx-products satellite::native_sequence::tests
```
