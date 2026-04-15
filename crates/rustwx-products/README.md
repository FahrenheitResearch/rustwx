# rustwx-products

Reusable workflow/product orchestration helpers for `rustwx`.

Current scope is intentionally conservative:

- proof cache helpers shared by CLI binaries
- shared HRRR fetch/decode/cache helpers for surface + pressure subsets
- shared projection/basemap assembly for cropped panel products
- shared Solar07 two-by-four panel rendering with header text

This crate exists so proof binaries stop owning fetch/decode/prep/render
assembly directly. Product-specific science still lives in `rustwx-calc`.

