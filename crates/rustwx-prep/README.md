# rustwx-prep

`rustwx-prep` contains preprocessing helpers that should not live in the renderer or the science wrappers.

## Current focus

The crate currently ports WRF-style lake cleanup logic:

- identify small connected water bodies from `LU_INDEX`
- build a small-water mask
- interpolate masked cells back from nearby land values

`area_threshold_km2` is applied to the physical area of each connected water body; water bodies strictly smaller than the threshold are masked.

## Why this crate exists

The existing WRF stack handles lake-friendly surface fields as preprocessing, not as a render trick. `rustwx-prep` is the place for that logic in the Rust-first stack.

## Current limits

- only lake-mask/interpolation work is implemented right now
- non-WRF land/sea-mask adapters are still future work

## Minimal example

```rust
use rustwx_core::GridShape;
use rustwx_prep::{WrfLakeMaskSpec, apply_wrf_lake_interpolation_f32};

let spec = WrfLakeMaskSpec::new(GridShape::new(100, 80)?, 3000.0, 3000.0, 50.0)?;
let corrected = apply_wrf_lake_interpolation_f32(&data, &lu_index, spec)?;
# let _ = corrected;
# Ok::<(), Box<dyn std::error::Error>>(())
```
