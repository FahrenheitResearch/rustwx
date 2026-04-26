# Proof artifacts

This directory holds curated proof outputs for the current `rustwx` state.

Included here:

- representative upper-air plots across multiple models
- the current HRRR severe proof panel
- native reflectivity + UH proofs
- sounding rendering proof
- timing JSON files for those runs
- one Python-vs-Rust upper-air benchmark

Excluded by `.gitignore`:

- fetch/decode caches
- GRIB subsets
- temporary scratch files
- intermediate confirmation files from iteration

Start here:

- `rustwx_hrrr_20260414_23z_f00_midwest_severe_proof_panel.png`
- `rustwx_gfs_20260414_18z_f000_conus_500mb_temperature_height_winds.png`
- `rustwx_ecmwf_open_data_20260414_12z_f000_conus_500mb_temperature_height_winds.png`
- `rustwx_hrrr_20260414_23z_f000_midwest_composite_reflectivity_uh.png`
- `rustwx_rrfs_a_20260414_23z_f000_midwest_composite_reflectivity_uh.png`
- `rustwx_sounding_demo_external_ecape.png`
- `python_gfs_500mb_benchmark.png`
