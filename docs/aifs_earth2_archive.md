# AIFS / Earth2 Archive Runbook

rustwx treats AIFS as a local archive model. Inference is done outside rustwx,
then NetCDF fields are placed into a stable layout that the normal map and
derived-product pipelines can read.

## Archive Layout

Set:

```powershell
$env:RUSTWX_EARTH2_ARCHIVE = "C:\Users\drew\aifs-vast\earth2_archive"
```

Files live at:

```text
{RUSTWX_EARTH2_ARCHIVE}/{model}/{YYYYMMDD}T{HH}Z/lead{HHH}.nc
```

Example:

```text
C:\Users\drew\aifs-vast\earth2_archive\aifs\20160822T00Z\lead024.nc
```

Use the writer to organize an already-generated Earth2Studio file:

```powershell
python scripts\earth2_archive_writer.py `
  --input-netcdf C:\Users\drew\aifs-vast\aifs_global_20160822T000000Z_lead024.nc `
  --archive-root C:\Users\drew\aifs-vast\earth2_archive `
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
1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50 hPa
```

`z{level}` is treated as geopotential in `m2/s2` and converted to height in
meters. `tcc/lcc/mcc/hcc` are treated as fractions and converted to percent.

## Smoke Commands

Direct product:

```powershell
$env:RUSTWX_EARTH2_ARCHIVE = "C:\Users\drew\aifs-vast\earth2_archive"
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 2m_temperature_10m_winds `
  --out-dir target\aifs-smoke --cache-dir target\aifs-smoke-cache --no-cache
```

Pressure product:

```powershell
cargo run -p rustwx-cli --bin direct_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe 500mb_temperature_height_winds `
  --out-dir target\aifs-smoke-pressure --cache-dir target\aifs-smoke-cache --no-cache
```

Derived product:

```powershell
cargo run -p rustwx-cli --bin derived_batch -- `
  --model aifs --date 20160822 --cycle 0 --forecast-hour 24 `
  --source earth2-archive --country usa `
  --recipe sbcape `
  --out-dir target\aifs-smoke-derived --cache-dir target\aifs-smoke-cache --no-cache
```

## Caveats

- AIFS is local-archive only; rustwx does not fetch it from NOMADS/AWS.
- The current archive reader supports AIFS-Single-style deterministic 2D
  fields. Ensembles or model-native levels should use the same archive source
  but need explicit schema additions.
- The sample AIFS file used for smoke tests does not include orography. Derived
  products therefore use a zero-terrain proxy until an orography field is added
  to the archive.
- Native model CAPE/CIN are not provided by this AIFS archive. Native-CAPE
  comparison products should remain blocked unless a future writer supplies
  explicit native diagnostics.
- Additional AI models should be added by extending the Earth2 archive variable
  map, not by adding another one-off render path.
