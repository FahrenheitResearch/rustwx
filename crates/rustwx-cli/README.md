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

These are currently the fastest way to validate new model/selector/render wiring.

## Current limits

- the main CLI and proof binaries are still separate
- proof binaries are specialized, not a final user-facing product interface

## Minimal example

```powershell
cargo run -p rustwx-cli -- list
```
