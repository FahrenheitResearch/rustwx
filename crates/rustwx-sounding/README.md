# rustwx-sounding

`rustwx-sounding` bridges generic sounding columns into `sharprs` and renders sounding PNGs.

## What is implemented

- validated generic sounding column types with conservative physical QC
- conversion to `sharprs` profiles
- native sounding rendering
- optional external ECAPE annotation block that can be appended to the rendered product

## Input validation

`SoundingColumn::validate()` checks the basic shape constraints and also rejects:

- non-finite values in the required profile vectors and optional omega vector
- pressure profiles that are not monotonic non-increasing
- height profiles that are not monotonic non-decreasing
- dewpoints that exceed temperature, while still allowing saturated levels

## Important note

`sharprs` is not being misrepresented here as an ECAPE engine. The current ECAPE support is explicit external annotation. That is deliberate.

## Current limits

- no direct model/observation ingest
- no internal ECAPE solver in the sounding stack
- sounding input assembly still belongs to higher-level crates

## Minimal example

```rust
use rustwx_sounding::write_full_sounding_png;

write_full_sounding_png(&column, "sounding.png")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```
