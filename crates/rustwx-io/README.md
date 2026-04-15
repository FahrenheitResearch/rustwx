# rustwx-io

`rustwx-io` is the fetch, probe, extract, and cache layer.

## Responsibilities

- source probing
- forecast-hour discovery
- `.idx`-driven byte-range fetch
- cached fetch results
- selector-backed GRIB extraction
- cached extracted fields

## What is implemented

- live source probes for supported models
- cached byte fetches
- structured GRIB extraction for the selector subset used by current proofs
- field cache layout organized by model/date/cycle/fhr/product/source/patterns

## Current limits

- extraction is still selector-driven rather than a broad general decoder
- volume-level APIs are not yet the default path
- this crate still relies on sibling local GRIB/download crates

## Minimal example

```rust
use rustwx_core::{CanonicalField, FieldSelector};
use rustwx_io::extract_field_from_bytes;

let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
let field = extract_field_from_bytes(&bytes, selector)?;
# let _ = field;
# Ok::<(), Box<dyn std::error::Error>>(())
```
