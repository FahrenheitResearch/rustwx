# CA Fire Public API Client Guide For Claude Code

This guide is for a Claude Code instance working on a client website, such as
cafire.org, that consumes the hosted CA Fire Weather API.

## Client Boundary

Use this public API base URL:

```text
https://cafire.wxsection.com
```

Use the JSON responses as the contract. Do not hardcode object-storage paths.
Always use URLs and metadata returned by the API.

## General Rules

- Fetch manifests first, then render UI from the manifest contents.
- Prefer `.webp` image URLs for browser previews and animation playback.
- Use `.png` URLs when the user opens/downloads a full-quality image.
- Expect products, loops, and metadata fields to grow over time.
- Keep cross sections, satellite, lightning, static maps, and meteograms as
  separate UI sections or tabs.
- Do not assume a missing lightning feature means the API is broken; zero
  California flashes is valid.
- Display model cycle, forecast hour, scan time, or lightning time window near
  the product being shown.
- Include an experimental-weather disclaimer in public-facing UI.

Suggested disclaimer:

```text
Experimental model visualization. Not an official forecast, warning,
evacuation, or safety-of-life product. Use official NWS, CAL FIRE, and local
emergency management sources for decisions.
```

## Endpoint Summary

```http
GET  /health
GET  /api/v1/public/latest-artifacts
GET  /api/v1/public/latest-diurnal-artifacts
GET  /api/v1/public/latest-lightning-artifacts
GET  /api/v1/public/latest-lightning.geojson
GET  /api/v1/public/latest-satellite-artifacts
GET  /api/v1/public/cross-section-products
POST /api/v1/public/cross-section-render
POST /api/v1/public/cross-section-loop
POST /api/v1/public/meteogram
POST /api/v1/public/meteogram.png
```

This guide focuses on cross sections, satellite products, and lightning
GeoJSON.

## Cross Sections

Cross sections are rendered by the API from HRRR pressure data. The client only
sends a route, product, forecast hour range, and image size. The API returns
cached or newly rendered image URLs.

### Product Catalog

Fetch the product list:

```http
GET /api/v1/public/cross-section-products
```

Expected shape:

```json
{
  "kind": "hrrr_pressure_cross_section_products",
  "schema_version": 1,
  "products": [
    { "product": "wind_speed", "label": "Wind Speed" }
  ],
  "excluded": [
    {
      "product": "smoke",
      "label": "Smoke",
      "reason": "not supported by the current HRRR pressure VolumeStore variable set"
    }
  ]
}
```

Use `products[].label` for UI text and `products[].product` for API requests.

Current product slugs:

```text
temperature
wind_speed
theta_e
rh
q
omega
vorticity
shear
lapse_rate
cloud
cloud_total
wetbulb
icing
frontogenesis
vpd
dewpoint_dep
moisture_transport
pv
fire_wx
```

### Still Render

Render one product or multiple products at one forecast hour:

```http
POST /api/v1/public/cross-section-render
Content-Type: application/json
```

Example:

```json
{
  "lat1": 40.5865,
  "lon1": -122.3917,
  "lat2": 39.0968,
  "lon2": -120.0324,
  "hour": 7,
  "products": "wind_speed",
  "spacing_km": 5,
  "top_pressure_hpa": 100,
  "width": 1400,
  "height": 820,
  "route_name": "Selected CA Cross-Section"
}
```

Important request fields:

```text
lat1, lon1       route start
lat2, lon2       route end
hour             forecast hour, usually 0-48
products         product slug or list of product slugs
spacing_km       sample spacing along route; 5 is a good default
top_pressure_hpa vertical top of plot; 100 is a good default
width, height    rendered image dimensions
route_name       optional client display label
force            optional; normally omit or false
```

Response fields to use:

```text
kind
generated_at_utc
store_cycle
date_yyyymmdd
cycle_utc
route
products
product_labels
forecast_hours
width
height
cache_hit
records[]
records[].product
records[].product_label
records[].hour
records[].webp_url
records[].png_url
records[].min_value
records[].max_value
records[].total_ms
manifest_url
```

For display, prefer `records[].webp_url || records[].png_url`.
Treat these as opaque API-returned URLs. Cross-section render outputs may be
served from the API node's local generated-artifact cache instead of object
storage.

### Loop Render

Render a frame sequence for one product over multiple forecast hours:

```http
POST /api/v1/public/cross-section-loop
Content-Type: application/json
```

Example:

```json
{
  "lat1": 40.5865,
  "lon1": -122.3917,
  "lat2": 39.0968,
  "lon2": -120.0324,
  "product": "wind_speed",
  "hours": "0-7",
  "spacing_km": 5,
  "top_pressure_hpa": 100,
  "width": 1400,
  "height": 820,
  "route_name": "Selected CA Cross-Section"
}
```

Use `"0-selectedHour"` for normal UI loop rendering. For example, if the user
selects f007, send `"hours": "0-7"`. Do not send `"all"` unless the UI clearly
means every available forecast hour.

Response fields to use:

```text
kind
loop
frame_count
frames[]
records[]
cache_hit
timing fields
manifest_url
```

Each `frames[]` item has the same useful image fields as `records[]`, including
`webp_url`, `png_url`, `hour`, `product`, and `product_label`.

Client playback pattern:

```js
const frames = report.frames || report.records || [];
const urls = frames.map((frame) => frame.webp_url || frame.png_url).filter(Boolean);
```

Then use those URLs in a scrubber, play/pause loop, or frame list. The API
returns frame URLs, not a guaranteed single animated file for cross sections.

### Cross-Section UI Pattern

1. Load `/api/v1/public/cross-section-products`.
2. Let the user draw or choose a California route.
3. Let the user choose product, forecast hour, top pressure, and size.
4. For a still, call `/cross-section-render`.
5. For a loop, call `/cross-section-loop` with `hours: "0-selectedHour"`.
6. Show `cache_hit`, `cycle_utc`, `forecast_hours`, and render timing in a
   small details panel if useful.
7. Handle `400` as invalid route/product/request.
8. Handle `429` as "render queue is busy; retry shortly".
9. Reuse returned URLs. Do not rebuild artifact URLs manually.

### Mapbox Cross-Section Route UI

Use Mapbox for route selection and context; use the API for the rendered
vertical cross-section image.

Client behavior:

1. Let the user click or drag two endpoints on the map.
2. Draw a GeoJSON `LineString` between the endpoints.
3. Add endpoint markers labeled A and B.
4. Submit those endpoint coordinates to the cross-section render endpoint.
5. Show the returned cross-section image beside or below the map.
6. Keep the map line visible while the image or loop plays.

Recommended Mapbox setup:

```js
map.addSource("mapbox-dem", {
  type: "raster-dem",
  url: "mapbox://mapbox.mapbox-terrain-dem-v1",
  tileSize: 512,
  maxzoom: 14
});

map.setTerrain({
  source: "mapbox-dem",
  exaggeration: 1.5
});

map.addSource("cafire-cross-section-route", {
  type: "geojson",
  lineMetrics: true,
  data: {
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: [[lon1, lat1], [lon2, lat2]]
    },
    properties: {}
  }
});

map.addLayer({
  id: "cafire-cross-section-route-casing",
  type: "line",
  source: "cafire-cross-section-route",
  layout: {
    "line-elevation-reference": "ground",
    "line-z-offset": 10
  },
  paint: {
    "line-color": "#ffffff",
    "line-width": 6,
    "line-opacity": 0.85
  }
});

map.addLayer({
  id: "cafire-cross-section-route",
  type: "line",
  source: "cafire-cross-section-route",
  layout: {
    "line-elevation-reference": "ground",
    "line-z-offset": 12
  },
  paint: {
    "line-color": "#d62828",
    "line-width": 3,
    "line-opacity": 0.95
  }
});
```

With terrain enabled, this renders the route as a line referenced to the ground
instead of as a purely flat overlay. The small `line-z-offset` keeps the route
visible just above the terrain surface. If a client uses a Mapbox GL JS version
that supports `line-cross-slope`, leave it unset so the elevated line follows
the terrain slope.

The older wxsection UI used a terrain-aware route concept: it sampled terrain
along the selected line and kept that same terrain profile fixed through loop
frames so the bottom boundary did not shift during playback. The API-rendered
cross-section images already include the terrain fill. If the client needs a
small elevation preview directly on the map screen, treat it as optional UI:
render a compact profile panel from route terrain metadata when that metadata
is available, but do not block the main render flow on it.

## Satellite Products

Satellite products are GOES-West ABI/GLM images generated by the API. The
client reads a manifest of current stills, size variants, loops, and optional
Mapbox image-overlay metadata.

### Latest Satellite Manifest

```http
GET /api/v1/public/latest-satellite-artifacts
```

Top-level fields to use:

```text
kind
generated_at_utc
satellite
abi_product
scan_id
scan_time_utc
scan_end_time_utc
domain
domain_label
bounds
products
product_catalog[]
artifacts[]
loops[]
loop_policy
source_keys
glm_source_keys
```

Use `scan_time_utc` as the timestamp shown to users.

### Product Names

Use names from `product_catalog[]` and `artifacts[].display_name`. Do not invent
client-side labels.

Expected website-facing products:

```text
GeoColor
GLM FED3+GeoColor
AirMass RGB
Sandwich RGB
Day Night Cloud Micro Combo RGB
Fire Temperature
Dust RGB
Band 1
Band 2
Band 3
Band 4
Band 5
Band 6
Band 7
Band 8
Band 9
Band 10
Band 11
Band 12
Band 13
Band 14
Band 15
Band 16
```

### Still Images

Each `artifacts[]` item may include:

```text
product
display_name
description
wavelength
category
display_order
png_url
webp_url
stills[]
still_widths[]
still_formats[]
mapbox_overlay
loops[]
```

Use this selection order:

1. For a thumbnail or gallery: choose the smallest `stills[]` item that is at
   least as wide as the display slot, preferring WebP.
2. For a full-size preview: use `webp_url || png_url`.
3. For download/full quality: use `png_url` when available.

Example helper:

```js
function bestStill(artifact, targetWidth = 600) {
  const stills = [...(artifact.stills || [])]
    .filter((item) => item.url)
    .sort((a, b) => {
      const aPenalty = a.format === "webp" ? 0 : 1;
      const bPenalty = b.format === "webp" ? 0 : 1;
      const aWidth = a.width || 99999;
      const bWidth = b.width || 99999;
      const aTooSmall = aWidth < targetWidth ? 100000 : 0;
      const bTooSmall = bWidth < targetWidth ? 100000 : 0;
      return aTooSmall + aPenalty + aWidth - (bTooSmall + bPenalty + bWidth);
    });
  return stills[0]?.url || artifact.webp_url || artifact.png_url;
}
```

### Satellite Loops

The satellite manifest exposes loops either at top level in `loops[]` and often
per artifact in `artifacts[].loops[]`.

Loop fields:

```text
product
display_name
format
url
duration_min
duration_label
frame_count
frame_ms
width
height
scan_times_utc[]
```

Prefer animated WebP. Use GIF only as a fallback where WebP animation is not
supported.

Example:

```js
function loopsForProduct(manifest, product) {
  const artifact = (manifest.artifacts || []).find((item) => item.product === product);
  return artifact?.loops || (manifest.loops || []).filter((loop) => loop.product === product);
}
```

### Mapbox Satellite Image Overlay

Some satellite artifacts include `mapbox_overlay`. If present, this is the
client's easiest georeferenced image-overlay path.

Use the returned metadata as-is:

```js
const overlay = artifact.mapbox_overlay;

map.addSource("cafire-satellite-geocolor", {
  type: "image",
  url: overlay.image_url || artifact.webp_url || artifact.png_url,
  coordinates: overlay.coordinates
});

map.addLayer({
  id: "cafire-satellite-geocolor",
  type: "raster",
  source: "cafire-satellite-geocolor",
  paint: {
    "raster-opacity": overlay.opacity ?? 0.75
  }
});
```

If `mapbox_overlay` is absent for a product, show it as a normal image gallery
item instead of guessing bounds.

### Satellite Polling

Poll the satellite manifest about every 5 minutes. If `scan_id` is unchanged,
keep the current UI and avoid reloading images.

## Lightning GeoJSON

Lightning has two public surfaces:

```http
GET /api/v1/public/latest-lightning-artifacts
GET /api/v1/public/latest-lightning.geojson
```

Use the artifacts endpoint for static image previews. Use the GeoJSON endpoint
for Mapbox.

### Static Lightning Artifact Manifest

```http
GET /api/v1/public/latest-lightning-artifacts
```

Useful fields:

```text
kind
generated_at_utc
satellite
domain
domain_label
time_window
flash_count_total
flash_count_in_domain
hours[0].uploaded[]
```

`hours[0].uploaded[]` contains PNG, WebP, and JSON artifacts. Prefer WebP for
preview and PNG for full-quality view.

### Lightning GeoJSON

```http
GET /api/v1/public/latest-lightning.geojson
```

Expected shape:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [-121.5, 38.2]
      },
      "properties": {
        "time_utc": "2026-04-28T22:10:00Z",
        "energy_j": 1234,
        "area_m2": 5678,
        "source_file": "OR_GLM-L2-LCFA..."
      }
    }
  ]
}
```

Coordinates are `[longitude, latitude]`.

Zero features is valid when there are no recent flashes inside the California
domain.

### Mapbox Lightning Layer

```js
map.addSource("cafire-lightning", {
  type: "geojson",
  data: "https://cafire.wxsection.com/api/v1/public/latest-lightning.geojson"
});

map.addLayer({
  id: "cafire-lightning",
  type: "circle",
  source: "cafire-lightning",
  paint: {
    "circle-radius": [
      "interpolate",
      ["linear"],
      ["coalesce", ["get", "energy_j"], 0],
      0, 3,
      5000, 7
    ],
    "circle-color": "#facc15",
    "circle-opacity": 0.85,
    "circle-stroke-color": "#7c2d12",
    "circle-stroke-width": 1
  }
});
```

Poll the GeoJSON every 30-60 seconds. In Mapbox GL JS, update the source:

```js
async function refreshLightning(map) {
  const source = map.getSource("cafire-lightning");
  if (!source) return;
  const response = await fetch("https://cafire.wxsection.com/api/v1/public/latest-lightning.geojson");
  source.setData(await response.json());
}
```

## Minimal Client Implementation Plan

1. Add an API module with `CAFIRE_API_BASE = "https://cafire.wxsection.com"`.
2. Add manifest fetchers for satellite, lightning artifacts, lightning GeoJSON,
   and cross-section products.
3. Add a satellite product gallery using `product_catalog`, `artifacts`, and
   `loops`.
4. Add optional Mapbox satellite image overlays using
   `artifact.mapbox_overlay`.
5. Add a lightning Mapbox layer using `latest-lightning.geojson`.
6. Add a cross-section route picker and product selector.
7. Add still and loop render actions using the cross-section endpoints.
8. Cache API responses client-side briefly to avoid repeated fetches while the
   user changes tabs.
9. Show API errors in the UI without exposing stack traces or implementation details.

## Error Handling

Recommended client behavior:

```text
HTTP 200  use response normally
HTTP 400  show "request outside supported California domain" or validation copy
HTTP 404  show "product not available yet"
HTTP 429  show "render queue busy; retry shortly"
HTTP 5xx  show "weather API temporarily unavailable"
network   show "could not reach weather API"
```

Do not retry render requests aggressively. For `429`, wait at least 15-30
seconds before retrying.

## Browser Verification Checklist

From the client site, verify:

- Satellite tab loads `GeoColor`, `GLM FED3+GeoColor`, RGB products, and Bands
  1-16 using API-provided labels.
- Satellite images use returned still URLs, not hardcoded paths.
- Satellite loops play from returned loop URLs.
- Satellite Mapbox overlay uses `mapbox_overlay.coordinates` exactly as
  returned.
- Lightning static image loads from `latest-lightning-artifacts`.
- Lightning Mapbox layer loads from `latest-lightning.geojson`.
- Lightning zero-feature state is handled cleanly.
- Cross-section product picker loads 19 products.
- Cross-section still render displays returned WebP/PNG.
- Cross-section loop renders `f000` through the selected forecast hour, not
  always all 49 frames.
- Existing static maps and meteogram UI still work.

## Quick API Checks

These are public API checks only:

```bash
BASE=https://cafire.wxsection.com

curl -fsS "$BASE/health"
curl -fsS "$BASE/api/v1/public/latest-satellite-artifacts"
curl -fsS "$BASE/api/v1/public/latest-lightning-artifacts"
curl -fsS "$BASE/api/v1/public/latest-lightning.geojson"
curl -fsS "$BASE/api/v1/public/cross-section-products"
```

Cross-section still test:

```bash
curl -fsS -X POST "$BASE/api/v1/public/cross-section-render" \
  -H 'Content-Type: application/json' \
  -d '{"lat1":40.5865,"lon1":-122.3917,"lat2":39.0968,"lon2":-120.0324,"hour":2,"products":"wind_speed","spacing_km":5,"top_pressure_hpa":100,"width":1400,"height":820,"route_name":"client_test"}'
```

Cross-section loop test:

```bash
curl -fsS -X POST "$BASE/api/v1/public/cross-section-loop" \
  -H 'Content-Type: application/json' \
  -d '{"lat1":40.5865,"lon1":-122.3917,"lat2":39.0968,"lon2":-120.0324,"product":"wind_speed","hours":"0-7","spacing_km":5,"top_pressure_hpa":100,"width":1400,"height":820,"route_name":"client_loop_test"}'
```

These commands are only public API contract checks.
