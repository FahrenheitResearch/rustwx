# rustwx-calc

`rustwx-calc` is the Rust-first diagnostics layer. It wraps `metrust` for gridded severe and thermodynamic products and exposes APIs shaped for Rust callers.

## What is implemented

- ECAPE-family grid APIs
- SB/ML/MU ECAPE triplet APIs
- failure-mask variants for ECAPE verification/debug
- CAPE/CIN wrappers
- SRH and bulk shear wrappers
- STP, SCP, EHI, local-proxy SHIP, and BRI wrappers
- bundled "supported severe" proof outputs

## Important notes

- the fixed STP path is the real fixed-layer form
- the current bundled SCP/EHI proof path is intentionally conservative
- the current SHIP wrapper matches the local `wrf-rust` hail-proxy formula and should not be treated as a canonical SHARPpy-style SHIP implementation yet
- full effective-layer severe support still depends on broader upstream profile logic

## Current limits

- this crate does not ingest model data
- it assumes the caller already has the required grid and profile inputs
- some severe products are still "supported proof fields" rather than final operational APIs

## Minimal example

```rust
use rustwx_calc::{EcapeGridInputs, EcapeOptions, compute_ecape};

let ecape = compute_ecape(inputs, &EcapeOptions::default())?;
# let _ = ecape;
# Ok::<(), Box<dyn std::error::Error>>(())
```
