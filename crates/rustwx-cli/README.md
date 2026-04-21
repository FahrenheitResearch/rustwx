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

- `direct_batch`
- `derived_batch`
- `ecape8_batch`
- `heavy_panel_hour`
- `plot_recipe_proof`
- `hrrr_ecape8`
- `hrrr_severe_proof`
- `hrrr_batch`
- `hrrr_direct_batch`
- `hrrr_derived_batch`
- `hrrr_windowed_batch`
- `non_ecape_hour`
- `product_catalog`
- `proof_gallery`

These are currently the fastest way to validate new model/selector/render wiring.
`derived_batch` / `hrrr_derived_batch` are now the canonical per-map lane for
derived, severe-style, and ECAPE-style products. `ecape8_batch` remains as a
convenience bundle runner for the full ECAPE family.

For HRRR operator-facing batch generation, the main non-ECAPE hour runner now
defaults to `NOMADS` and the full-family ingest path.

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
cargo run -p rustwx-cli --bin direct_batch -- --model gfs --all-supported --date 20260414 --cycle 18 --forecast-hour 12 --region midwest
```

```powershell
cargo run -p rustwx-cli --bin derived_batch -- --model rrfs-a --all-supported --date 20260414 --cycle 20 --forecast-hour 2 --source aws --region midwest
```

```powershell
cargo run -p rustwx-cli --bin derived_batch -- --model ecmwf-open-data --recipe sbecape,mlecape,muecape,sbncape,sbecin,mlecin,ecape_scp,ecape_ehi --date 20260414 --cycle 12 --forecast-hour 6 --source ecmwf --region midwest
```

```powershell
cargo run -p rustwx-cli --bin heavy_panel_hour -- --model hrrr --date 20260414 --cycle 23 --forecast-hour 1 --region midwest
```

```powershell
cargo run -p rustwx-cli --bin hrrr_direct_batch -- --recipe 500mb_temperature_height_winds,700mb_temperature_height_winds,composite_reflectivity
```

```powershell
cargo run -p rustwx-cli --bin hrrr_derived_batch -- --recipe sbcape,stp_fixed,sbecape,ecape_scp,temperature_advection_700mb
```

```powershell
cargo run -p rustwx-cli --bin hrrr_windowed_batch -- --forecast-hour 6 --product qpf6h,qpf-total,uh25km-run-max
```

```powershell
cargo run -p rustwx-cli --release --bin hrrr_non_ecape_hour -- --date 20260414 --cycle 23 --forecast-hour 1 --region conus
```

```powershell
cargo run -p rustwx-cli --release --bin non_ecape_hour -- --model gfs --date 20260414 --cycle 18 --forecast-hour 12 --region midwest
```

```powershell
cargo run -p rustwx-cli --bin product_catalog -- --out C:\Users\drew\rustwx\proof\product_catalog.json
```

```powershell
cargo run -p rustwx-cli --bin proof_gallery -- --proof-root C:\Users\drew\rustwx\proof --out-dir C:\Users\drew\rustwx\proof\viewer
```
