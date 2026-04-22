# rustwx-python

`rustwx-python` is the optional Python binding crate for `rustwx`.

## Design goal

Keep Python convenient, keep the hot path in Rust, and expose generic render/model metadata surfaces that are usable outside WRF-specific callers.

## What is implemented

With the `python` feature enabled, the module exposes:

- model listing and source/model helpers
- projected-grid rendering via `render_projected_map` and `render_projected_map_json`
- compatibility aliases `render_wrf_map` and `render_wrf_map_json`
- standalone projected projection metadata via `describe_projected_projection`
- standalone projected grid/layout metadata via `describe_projected_geometry`
- standalone projected CONUS basemap overlay extraction via `build_projected_basemap_overlays`
- future-facing cross-section request validation/normalization via `normalize_cross_section_request`

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
import rustwx_python

print(rustwx_python.list_models_json())
```

## Projected render example

```python
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

metadata = rustwx_python.render_projected_map(spec, lat, lon, field)
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

geometry = rustwx_python.describe_projected_geometry(
    surface,
    lat,
    lon,
    include_projected_domain=False,
)
overlays = rustwx_python.build_projected_basemap_overlays(
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
xsect = rustwx_python.normalize_cross_section_request(
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
- full fetch/download/render orchestration is still outside this binding layer
