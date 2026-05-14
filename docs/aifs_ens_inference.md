# AIFS ENS v2 Inference Archive

RustWX treats locally inferred AIFS ENS v2 NetCDFs as a first-class
`aifs-inference` archive source.

Supported member-slice layout:

```text
aifs_long_YYYYMMDDTHH0000Z_mMM_leadFFFFF.nc
```

Example:

```text
aifs_long_20260513T060000Z_m00_lead00006.nc
```

Set the archive root before rendering:

```powershell
$env:RUSTWX_AIFS_INFERENCE_ARCHIVE = "C:\path\to\aifs2yr_ens2_netcdfs"
```

Member render:

```powershell
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --source aifs-inference --date 20260513 --cycle 6 `
  --forecast-hour 6 --region global `
  --recipe 2m_temperature_10m_winds --member 0 `
  --out-dir target\aifs_ens\members\m00
```

Derived member render:

```powershell
cargo run -p rustwx-cli --bin derived_batch -- `
  --model aifs --source aifs-inference --date 20260513 --cycle 6 `
  --forecast-hour 6 --region global `
  --recipe bulk_shear_0_6km --member 0 `
  --out-dir target\aifs_ens\members\m00
```

Ensemble mean/stat render across separate member files:

```powershell
cargo run -p rustwx-cli --bin grib_ensemble_reduce -- `
  --model aifs --source aifs-inference --date 20260513 --cycle 6 `
  --forecast-hour 6 --region global `
  --recipe 2m_temperature_10m_winds --stat mean `
  --member-template "m{member}" --member 00,01,02,03,04 `
  --out-dir target\aifs_ens\stats\mean
```

Probability render:

```powershell
cargo run -p rustwx-cli --bin grib_ensemble_reduce -- `
  --model aifs --source aifs-inference --date 20260513 --cycle 6 `
  --forecast-hour 6 --region global `
  --recipe 2m_temperature_10m_winds --stat prob-exceed `
  --threshold 30 --threshold-op gt `
  --member-template "m{member}" --member 00,01,02,03,04 `
  --out-dir target\aifs_ens\prob\t2m_gt30c
```

For temperature and dewpoint probability products, Celsius thresholds are
accepted when the extracted AIFS field is Kelvin. For example `--threshold 30`
means `> 30 C` for 2 m temperature and 2 m dewpoint recipes.

Runner template:

```text
rustwx-runner/config/runner_aifs_ens_local.toml
```

That template uses the same local archive source, member products (`m00`...
`m04`), direct member fanout, and direct ensemble-stat/probability fanout.
