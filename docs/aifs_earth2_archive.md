# AIFS Local NetCDF Archive Runbook

rustwx treats operational AIFS Single v2 as an ECMWF open-data model by
default. Local AIFS NetCDFs are first-class through two explicit local sources:

- `aifs-inference` for actively generated/inferenced AIFS-v2 NetCDFs that are
  disseminated into a local archive while rustwx-runner watches and plots them.
- `earth2-archive` for older Earth2Archive-style experimental or hindcast
  NetCDF runs.

Both sources use the normal rustwx direct, derived, WxStore-grid-export, and
runner plotting paths. They are not separate plotting scripts.

## Archive Layout

Set:

```powershell
$env:RUSTWX_AIFS_INFERENCE_ARCHIVE = "$env:RUSTWX_RUNNER_DATA\aifs_inference_archive"
$env:RUSTWX_EARTH2_ARCHIVE = "$env:USERPROFILE\aifs-earth2-archive"
```

Files live at:

```text
{RUSTWX_AIFS_INFERENCE_ARCHIVE}/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc
{RUSTWX_EARTH2_ARCHIVE}/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc
```

`HHH` is a minimum width, not a hard cap. Long experimental integrations use
the same layout, for example `lead8640.nc` for a 360-day run.

Example:

```text
C:\wxdata\aifs-earth2-archive\aifs\20160822T00Z\lead024.nc
```

Use the writer to organize an already-generated Earth2Studio file:

```powershell
python scripts\earth2_archive_writer.py `
  --input-netcdf C:\wxdata\aifs-input\aifs_global_20160822T000000Z_lead024.nc `
  --archive-root C:\wxdata\aifs-earth2-archive `
  --model aifs `
  --init-time 2016-08-22T00:00:00Z `
  --lead 24
```

## Expected NetCDF Shape

The current reader expects CF-style 2D variables on:

```text
lat(lat), lon(lon)
```

with fields shaped `lat x lon`. Longitude is normalized and reordered to
`-180..180` internally so global 0..360 files can render through the same
domain paths as HRRR/GFS.

Supported AIFS variables now include:

```text
t2m, d2m, u10m, v10m, sp, msl, tcw, tcc, lcc, mcc, hcc, tp06
t{level}, q{level}, u{level}, v{level}, w{level}, z{level}
```

Pressure levels currently recognized:

```text
1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50, 10 hPa
```

`z{level}` is treated as geopotential in `m2/s2` and converted to height in
meters. `tcc/lcc/mcc/hcc` are treated as fractions and converted to percent.

## Smoke Commands

Operational AIFS Single v2 open data:

```powershell
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20260512 --cycle 12 --forecast-hour 6 `
  --source ecmwf --region global `
  --recipe 2m_temperature_10m_winds `
  --out-dir target\aifs-v2-smoke --cache-dir target\aifs-v2-smoke-cache
```

Direct product:

```powershell
$env:RUSTWX_AIFS_INFERENCE_ARCHIVE = "$env:RUSTWX_RUNNER_DATA\aifs_inference_archive"
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20260512 --cycle 12 --forecast-hour 6 `
  --source aifs-inference --region conus `
  --recipe 2m_temperature_10m_winds `
  --out-dir target\aifs-inference-smoke --cache-dir target\aifs-inference-smoke-cache --no-cache

$env:RUSTWX_EARTH2_ARCHIVE = "$env:USERPROFILE\aifs-earth2-archive"
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 2m_temperature_10m_winds `
  --out-dir target\aifs-smoke --cache-dir target\aifs-smoke-cache --no-cache
```

If `--cycle` is omitted, rustwx scans
`$RUSTWX_EARTH2_ARCHIVE\aifs\*\lead{forecast_hour}.nc` and picks the newest
cycle at or before the requested date. Passing `--cycle` is still preferred for
reproducible research runs.

For local Earth2Archive AIFS runs, rustwx allows six-hourly forecast leads out
to `f43848`, a five-calendar-year horizon with leap-day slack. Other
operational models keep their normal shorter forecast-hour validation.

Pressure product:

```powershell
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 500mb_temperature_height_winds `
  --out-dir target\aifs-smoke-pressure --cache-dir target\aifs-smoke-cache --no-cache
```

Explicit member/stat direct products:

```powershell
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 2m_temperature_10m_winds `
  --member 1 `
  --out-dir target\aifs-smoke-member --cache-dir target\aifs-smoke-cache --no-cache

cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 2m_temperature_10m_winds `
  --stat mean `
  --out-dir target\aifs-smoke-mean --cache-dir target\aifs-smoke-cache --no-cache
```

The Python/agent CLI accepts the same direct-map selector:

```powershell
rustwx render-maps --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --product 2m_temperature_10m_winds --domain conus --stat mean
```

Derived product:

```powershell
cargo run -p rustwx-cli --bin non_ecape_hour -- `
  --model aifs --date 20260512 --cycle 12 --forecast-hour 6 `
  --source aifs-inference --region conus --all-supported --skip-windowed `
  --out-dir target\aifs-inference-non-ecape

cargo run -p rustwx-cli --bin derived_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe sbcape `
  --out-dir target\aifs-smoke-derived --cache-dir target\aifs-smoke-cache --no-cache
```

## Caveats

- Operational AIFS Single v2 is fetched from ECMWF open data. Earth2Archive is
  still the explicit source for legacy local NetCDF experiments and long
  hindcast runs. Use `aifs-inference` for active inferred AIFS-v2 output.
- The archive reader supports AIFS-Single-style deterministic 2D fields and
  direct-map member/stat selection for files shaped `(member, lat, lon)`.
  Deterministic requests against a member-shaped field intentionally select
  member 0 for backward compatibility.
- `--member N` selects a specific member. `--stat mean|std|min|max|p10|p50|p90`
  first looks for a precomputed sibling such as `t2m_mean`; if it is absent, it
  computes the statistic from the member axis on demand for direct maps.
- Ensemble selection is currently direct-product only. Derived/heavy/windowed
  products and probability-of-exceedance recipes remain future work because they
  need validated ensemble NetCDF fixtures and product semantics.
- The sample AIFS file used for smoke tests does not include orography. Derived
  products therefore use a zero-terrain proxy until an orography field is added
  to the archive.
- Native model CAPE/CIN are not provided by this AIFS archive. Native-CAPE
  comparison products should remain blocked unless a future writer supplies
  explicit native diagnostics.
- Additional AI models should be added by extending the Earth2 archive variable
  map, not by adding another one-off render path.
