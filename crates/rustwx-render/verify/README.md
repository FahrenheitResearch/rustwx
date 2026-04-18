# rustwx-render-verify

`rustwx-render-verify` is a small standalone verification crate for render behavior.

It is not a normal workspace member. It exists so rendering behavior can be tested in isolation against the shipped `rustwx-render` crate without pulling the whole `rustwx` workspace into every check.

## What it covers

- render request sanity
- Solar07 scale wiring
- PNG output sanity
- regression-style render assertions

## Current limits

- this is a narrow verification harness, not the production render API
- it is intentionally minimal and just re-exports `rustwx-render`

## Minimal example

```powershell
cargo test --manifest-path crates/rustwx-render/verify/Cargo.toml
```
