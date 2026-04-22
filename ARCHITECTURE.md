# Weather-Native Plot Engine Architecture

This workspace is split along stable engine boundaries instead of one-off plotting scripts.

## Current boundaries

- `rustwx-products` owns model-aware fetch, decode, crop/window choice, meteorological product assembly, and model-specific cross-section field construction.
- `rustwx-render` owns projected map requests, layout, overlays, labels, colorbars, and PNG emission.
- `rustwx-contour` owns contour topology extraction used by the native projected contour-fill path.
- `rustwx-cross-section` owns section paths, sampling, vertical axes, terrain masking, lightweight section rendering, and the public cross-section palette/style catalog.
- `rustwx-python` stays thin and exposes engine-facing bindings without moving product science back into Python.

## What is real now

- Native projection metadata is preserved from decode through projected rendering instead of being collapsed into a generic map frame.
- The standard overlay story is unchanged: shared map labels, basemap overlays, colorbars, and composition still come from `rustwx-render`.
- A separate native projected contour-fill path, built on `rustwx-contour`, is now live in the derived-product lane for `stp_fixed`, `sbcape`, `mlcape`, `srh_0_1km`, `srh_0_3km`, `ehi_0_1km`, and `ehi_0_3km`.
- That native contour-fill path is not yet the shared live map contour backend for every contour-sensitive product. Direct synoptic products such as `mslp_10m_winds` still ride the existing projected render/overlay path and stay in the proof suite as comparison targets.
- `rustwx-cross-section` now exposes a public palette/style surface informed by `wxsection_ref` through `CrossSectionPalette`, `CrossSectionProduct`, and `CrossSectionStyle`.
- `rustwx-products::cross_section` now builds real gridded pressure sections from decoded model data instead of leaving section science inside a proof script. The first supported family is `temperature`, `relative_humidity`, `theta_e`, and `wind_speed`.
- The current real-data HRRR section lane now renders a small multi-product family with terrain, product-aware palettes, product-aware colorbars, and section-relative wind overlay. The temperature proof uses the `temperature_white_zero` palette as the current hero section.
- `hrrr_native_proof` still defaults to a tight `conus_contour` suite case for maps, but its cross-section hook lane now emits a bounded multi-proof section set instead of a single temperature-only artifact.

## Near-term gaps

- Broaden native projected contour-fill coverage beyond the current derived set, especially direct contour-sensitive synoptic products.
- Add more cross-section products from the `wxsection_ref` catalog once the upstream gridded data path exposes the right fields cleanly, especially omega and winter/cloud diagnostics.
- Move beyond the current pressure-axis family into richer overlay combinations and optional alternate vertical coordinates where the data path supports them cleanly.
- Keep aligning Rust and Python around the same product/layout vocabulary rather than parallel request shapes.
