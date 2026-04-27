# rustwx Python bindings

`rustwx` is the optional Python binding package for the Rust-first `rustwx`
weather workspace.

## Design goal

Keep Python convenient, keep the hot path in Rust, and expose generic render/model metadata surfaces that are usable outside WRF-specific callers.

## What is implemented

With the `python` feature enabled, the module exposes:

- agent-facing discovery and map rendering via `agent_capabilities_json`,
  `list_domains_json`, `render_maps_json`, `render_glm_lightning_json`, and
  `sample_point_timeseries_json`
- model listing and source/model helpers
- projected-grid rendering via `render_projected_map` and `render_projected_map_json`
- compatibility aliases `render_wrf_map` and `render_wrf_map_json`
- standalone projected projection metadata via `describe_projected_projection`
- standalone projected grid/layout metadata via `describe_projected_geometry`
- standalone projected CONUS basemap overlay extraction via `build_projected_basemap_overlays`
- future-facing cross-section request validation/normalization via `normalize_cross_section_request`
- native sounding-column rendering via `render_sounding_column` and `render_sounding_column_json`

The wheel also installs a stable `rustwx` console command for agent and MCP
adapters:

```powershell
rustwx capabilities
rustwx list-domains --kind country --limit 5
rustwx render-maps --date 20260424 --model hrrr --domain california --product 2m_temperature_10m_winds --out-dir out
rustwx render-lightning --domain california --data-dir C:\Users\drew\lightning-test\data\glm --out-dir out
rustwx sample-point-timeseries --date 20260427 --cycle 0 --lat 40.802 --lon -124.164 --forecast-hour-end 6
```

`render-maps` accepts mixed product slugs and routes them to the appropriate
direct, light derived, heavy ECAPE-derived, or HRRR windowed product path. Heavy
ECAPE slugs such as `sbecape`, `mlecape`, `muecape`, ECAPE/CAPE ratios, NCAPE,
ECIN, and ECAPE EHI/SCP/STP use the canonical `derived_batch` ECAPE path; they
do not require callers to discover or run separate binaries.

When `cache_dir` / `--cache-dir` is omitted, the agent API uses a shared
`rustwx_outputs/cache` fetch/decode cache, or `RUSTWX_CACHE_DIR` when that
environment variable is set. The cache is intentionally independent of
`out_dir` and map bounds, so city or bbox sweeps can reuse the same upstream
GRIB fetches while writing PNGs into different output folders.

MCP servers should call these stable Python/CLI entry points instead of invoking
internal proof binaries.

`render_glm_lightning_json` / `rustwx render-lightning` reads GOES GLM
`OR_GLM-L2-LCFA_*.nc` files, renders native Rust projected flash maps, and
writes a JSON flash artifact with lat/lon, time, energy, and area fields for
agent consumption.

`sample_point_timeseries_json` samples native model fields at a lat/lon over a
forecast-hour range for meteograms and point-and-click agent tools. The default
variable set is HRRR-meteogram ready: 2 m T/Td/Tw/RH, 10 m wind/gust, hourly
and accumulated QPF, low/mid/high clouds, MSLP, VPD, HDW, and the fire-weather
composite. It uses rustwx's GRIB/idx fetch path and shared cache rather than
cfgrib/xarray.

Every new projected helper has both a Python-object entry point and a `_json` variant:

- Python-object entry points accept either a JSON string or a JSON-serializable Python `dict`
- `_json` entry points keep returning pretty JSON strings for low-friction interop

## Projected map API

The projected map surface is generic and public-facing. The caller supplies:

- `lat`, `lon`, `field` as `numpy.ndarray` 2-D arrays
- a render spec with product metadata, color scale, layout, and projection metadata
- optional contour, overlay, and wind layers

`render_projected_map(...)` writes the PNG and returns a Python `dict` with:

- typed `projection`, `extents`, `layout`, and `layers` sections
- legacy `pixel_bounds`, `data_extent`, `valid_data_extent`, and `projection_info` keys for compatibility

## Minimal example

```python
import rustwx

print(rustwx.list_models_json())
```

## Point time-series example

```python
import json
import rustwx

report = rustwx.sample_point_timeseries_json(json.dumps({
    "model": "hrrr",
    "date_yyyymmdd": "20260427",
    "cycle_utc": 0,
    "source": "nomads",
    "lat": 40.802,
    "lon": -124.164,
    "forecast_hour_start": 0,
    "forecast_hour_end": 6,
    "variables": ["temperature_2m_c", "relative_humidity_2m_pct", "wind_speed_10m_ms", "hdw"],
}))
print(report)
```

## Projected render example

```python
import rustwx

spec = {
    "output_path": "example.png",
    "product_key": "Example",
    "field_units": "dBZ",
    "scale": {
        "kind": "palette",
        "palette": "reflectivity",
        "levels": [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        "extend": "Both",
    },
    "projection": {
        "map_proj": 1,
        "truelat1": 30.0,
        "truelat2": 60.0,
        "stand_lon": -97.0,
        "cen_lat": 38.0,
        "cen_lon": -97.0,
    },
    "width": 1100,
    "height": 850,
    "basemap_style": "none",
}

metadata = rustwx.render_projected_map(spec, lat, lon, field)
print(metadata["projection"]["kind"])
print(metadata["pixel_bounds"])
```

## Geometry and overlay metadata example

```python
surface = {
    "projection": spec["projection"],
    "width": 1100,
    "height": 850,
    "visual_mode": "filled_meteorology",
    "basemap_style": "filled",
}

geometry = rustwx.describe_projected_geometry(
    surface,
    lat,
    lon,
    include_projected_domain=False,
)
overlays = rustwx.build_projected_basemap_overlays(
    surface,
    lat,
    lon,
    include_geometry=False,
)

print(geometry["extents"]["padded"])
print(overlays["counts"])
```

## Cross-section request normalization example

`normalize_cross_section_request(...)` does not render a cross-section yet. It validates and fills defaults for a future shared cross-section API surface.

```python
xsect = rustwx.normalize_cross_section_request(
    {
        "path": {
            "start": {"lat": 39.74, "lon": -104.99, "label": "Denver"},
            "end": {"lat": 41.88, "lon": -87.63, "label": "Chicago"},
        },
        "field": {"product_key": "temperature", "field_units": "degC"},
    }
)

print(xsect["path_metrics"])
print(xsect["request"]["axis"])
```

## Current limits

- projected rendering still expects caller-owned arrays
- cross-section support is validation/normalization only in this crate
- `render_maps_json` covers model fetch/download/render orchestration for
  direct, derived, heavy ECAPE-derived, and HRRR windowed map products
- `sample_point_timeseries_json` is a point data primitive; consumer-specific
  meteogram styling stays outside the Python wheel
- sounding rendering expects a caller-supplied validated column; model fetch and
  lat/lon extraction live in the Rust CLI for now
