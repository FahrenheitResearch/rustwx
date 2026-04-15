# rustwx TODO

## Phase 1: ingest and caching

- Add a real `rustwx-io-grib` layer on top of `ecrust`
- Support byte-range download, decode, and on-disk field caches
- Keep `cfrust` as optional Python/xarray convenience only
- Add latest-run probing that handles ECMWF quirks cleanly
- Decide whether `rustwx-io` owns source fallback policy directly or via a higher product builder

## Phase 2: science

- Lift or wrap `metrust` severe diagnostics into Rust-first product builders
- Add the "easy path" APIs for ECAPE-family grids so callers do not hand-build parcel inputs
- Wire ECAPE-family products into `sharprs` as explicit external annotations first
- Add parity fixtures against `metrust` and selected MetPy references

## Phase 3: map products

- Build product assembly on top of `rustwx-render`
- Add proper projection metadata instead of synthetic demo geometry
- Wire `rustwx-prep` lake-aware surface thermo correction into the surface/severe product builders
- Add panel composition and GIF/animation output in Rust

## Phase 4: Python layer

- Keep bindings thin and optional
- Bind stable Rust APIs only
- Avoid Python data marshaling in the hot path
