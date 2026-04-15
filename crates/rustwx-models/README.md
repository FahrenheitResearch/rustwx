# rustwx-models

`rustwx-models` is the model registry and source-planning crate.

## Responsibilities

- built-in model definitions
- source ordering and source metadata
- URL builders
- latest-run probing
- forecast-hour availability probes
- plot-recipe fetch planning

## Built-in models

- `HRRR`
- `GFS`
- `ECMWF open data`
- `RRFS-A`

## What is implemented

- URL resolution for all built-in models
- NOAA-style latest/probe/hour checks
- recipe planning for selector-backed upper-air plots
- explicit support/blocker reporting when a recipe is not wired for a model

## Current limits

- ECMWF latest-run probing is still weaker than the NOAA feeds
- recipe coverage is not uniform across all models
- some native convective and severe recipes are still HRRR/RRFS-A only

## Minimal example

```rust
use rustwx_core::{CycleSpec, ModelId, ModelRunRequest};

let request = ModelRunRequest::new(
    ModelId::Gfs,
    CycleSpec::new("20260414", 18)?,
    0,
    "pgrb2.0p25",
)?;
let urls = rustwx_models::resolve_urls(&request)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```
