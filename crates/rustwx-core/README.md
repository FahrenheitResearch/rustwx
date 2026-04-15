# rustwx-core

`rustwx-core` defines the shared domain model for the workspace.

## Responsibilities

- model and source identifiers
- cycle and timestep request types
- grid and field containers
- canonical field names
- vertical selectors
- validation and common errors

## What is implemented

- `GridShape`, `LatLonGrid`, and typed 2D/3D field containers
- `CanonicalField` and `FieldSelector`
- model/source/time request types used by the fetch and registry layers
- selector support for the currently wired upper-air and native convective proofs

## Current limits

- selector coverage is still intentionally narrow
- projection metadata is still lighter than the eventual end-state
- this crate does not know anything about fetch or rendering

## Minimal example

```rust
use rustwx_core::{CanonicalField, FieldSelector};

let selector = FieldSelector::isobaric(CanonicalField::Temperature, 500);
assert_eq!(selector.to_string(), "temperature_500_mb");
```
