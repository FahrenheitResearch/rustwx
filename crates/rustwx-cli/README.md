# rustwx-cli

`rustwx-cli` is the command-line entrypoint plus the proof binaries used to exercise the stack end to end.

## Main CLI

Current top-level commands:

- `list`
- `show`
- `url`
- `latest`
- `hours`
- `probe`
- `fetch`

## Proof binaries

This crate also contains targeted proof executables such as:

- `plot_recipe_proof`
- `hrrr_ecape8`
- `hrrr_severe_proof`
- `hrrr_batch`
- `hrrr_direct_batch`
- `hrrr_derived_batch`
- `hrrr_windowed_batch`
- `product_catalog`
- `proof_gallery`

These are currently the fastest way to validate new model/selector/render wiring.

## Current limits

- the main CLI and proof binaries are still separate
- proof binaries are specialized, not a final user-facing product interface

## Minimal example

```powershell
cargo run -p rustwx-cli -- list
```

```powershell
cargo run -p rustwx-cli --bin hrrr_batch -- --product severe-proof,ecape8
```

```powershell
cargo run -p rustwx-cli --bin hrrr_direct_batch -- --recipe 500mb_temperature_height_winds,700mb_temperature_height_winds,composite_reflectivity
```

```powershell
cargo run -p rustwx-cli --bin hrrr_derived_batch -- --recipe sbcape,stp_fixed,temperature_advection_700mb
```

```powershell
cargo run -p rustwx-cli --bin hrrr_windowed_batch -- --forecast-hour 6 --product qpf6h,qpf-total,uh25km-run-max
```

```powershell
cargo run -p rustwx-cli --bin product_catalog -- --out C:\Users\drew\rustwx\proof\product_catalog.json
```

```powershell
cargo run -p rustwx-cli --bin proof_gallery -- --proof-root C:\Users\drew\rustwx\proof --out-dir C:\Users\drew\rustwx\proof\viewer
```
