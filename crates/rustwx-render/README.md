# rustwx-render

`rustwx-render` is the Rust map-rendering crate for `rustwx`.

It wraps the local `wrf-render` engine with `rustwx`-level request types, Solar07 palettes, contour layers, barb layers, and panel helpers.

## What is implemented

- Solar07-backed filled maps
- contour overlays
- wind barb overlays
- projected line overlays
- PNG and image rendering
- multi-panel composition helpers

## What this crate expects from callers

- the field values to render
- the grid definition
- projected coordinates if a projected map is being drawn
- any line overlays or contour/barb layers

## Current limits

- no built-in model projection pipeline
- no animation/GIF orchestration yet
- no fetch/decode logic here by design

## Minimal example

```rust
use rustwx_render::{MapRenderRequest, Solar07Product, save_png};

let request = MapRenderRequest::for_solar07_product(field, Solar07Product::Sbecape);
save_png(&request, "out.png")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```
