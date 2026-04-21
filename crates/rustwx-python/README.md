# rustwx-python

`rustwx-python` is the optional thin Python layer for `rustwx`.

## Design goal

Keep Python convenient but thin. The hot path should stay in Rust.
Today that means the crate depends on the core/model/io path plus optional render bindings through `pyo3`.

## What is implemented

With the `python` feature enabled, the module currently exposes:

- model listing
- URL resolution
- latest-run probing
- forecast-hour availability
- source probing
- projected-grid map rendering via `render_projected_map_json`
- compatibility alias `render_wrf_map_json` for existing WRF-Runner callers

The current Python API intentionally returns JSON strings so the Rust surface can keep moving without committing to a wide Python object model too early.

## Current limits

- projected rendering is a low-level bridge that expects arrays plus projection metadata from the caller
- no typed Python objects yet
- no full fetch/download workflow bindings yet

## Minimal example

```python
import rustwx_python

print(rustwx_python.list_models_json())
```

## Local projected-model example

```python
import json
import rustwx_python

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

metadata_json = rustwx_python.render_projected_map_json(
    json.dumps(spec),
    lat,
    lon,
    field,
)
print(metadata_json)
```
