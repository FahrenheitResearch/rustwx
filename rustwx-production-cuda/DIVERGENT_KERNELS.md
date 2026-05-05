# Deferred kernels

Calcs where met-cu and metrust disagree numerically. Per project policy
(metrust = authoritative reference) we defer these rather than introducing
silent disagreement. A future pass will reconcile each one and re-port.

## Audit summary

The earlier `REVIEW.md` divergence list overstated the problem. After
re-reading both code paths, **STP, SCP, Haines, and hot_dry_windy port
as-is** — they are byte-identical to metrust. The four below remain.

| Calc | Met-cu source | Metrust source | Nature | Magnitude |
|---|---|---|---|---|
| `smooth_n_point` | `met-cu/python/metcu/kernels/grid.py:1119` | `vendor/metrust/src/calc/smooth.rs:463` | Different 9-pt weights (custom 0.40/0.10/0.05 vs MetPy 0.25/0.125/0.0625) | max diff 1.5 K, rtol 0.30 |
| `showalter_index` | `met-cu/python/metcu/kernels/wind.py:1232` | `vendor/metrust/src/calc/thermo.rs:300` | RK4 substep size mismatch on moist adiabat | ~7%, rtol 0.15 |
| `cin` | `met-cu/python/metcu/kernels/thermo.py:1539` | `vendor/wx-math/src/thermo.rs:1878` | met-cu bounds CIN to LFC region (correct); metrust v0.3.x integrates unbounded → can hit -30,000 J/kg | up to 30 kJ/kg |
| `dcp` | TBD (couldn't locate kernel) | TBD | Reported 4th-term formula diff (mixing_ratio/11 vs mean_wind/16) | unverified |
| `heat_index` | `met-cu/python/metcu/kernels/thermo.py:1016` | `vendor/metrust/src/calc/atmo.rs:223` | Threshold rule differs near 80 F: kernel uses `if t_f < 80 → steadman`; metrust averages Steadman with T_F first (`hi_avg = (steadman + t_f) / 2`) and then checks `< 80`. Gives slightly different values for T_F in roughly 76..82 F. | ~ a few tenths of K near boundary |
| `apparent_temperature` | `met-cu/python/metcu/kernels/thermo.py:1107` | `vendor/metrust/src/calc/atmo.rs:299` | Heat-index branch uses raw Rothfusz (no Steadman/T_F average); inherits `heat_index` divergence at the 80 F boundary. | ~ a few tenths of K near 80 F |
| `altimeter_to_sea_level_pressure` | `met-cu/python/metcu/kernels/thermo.py:748` | `vendor/metrust/src/calc/atmo.rs:165` | Step-1 uses simple-ratio + 0.3, while metrust uses the Smithsonian inverse `(alt^n - p0^n*L*H/T0)^(1/n) + 0.3`. Step-2 uses Rd=287.058 in both. | up to tens of Pa at non-zero elevation |
| `altimeter_to_station_pressure` | `met-cu/python/metcu/kernels/thermo.py:772` | `vendor/metrust/src/calc/atmo.rs:107` | Kernel uses `alt * ratio^(1/ROCP)` (matches wx_math). metrust uses Smithsonian `(alt^n - p0^n*L*H/T0)^(1/n) + 0.3`. | up to ~1 hPa at high elevation |
| `station_to_altimeter_pressure` | `met-cu/python/metcu/kernels/thermo.py:788` | `vendor/metrust/src/calc/atmo.rs:127` | Kernel uses literal `BARO_EXP = 0.190284`; metrust recomputes BARO_EXP = G*M_air/(R_star*L) ≈ 0.19026308. | ~1e-5 relative |
| `mean_wind` (column) | `met-cu/python/metcu/kernels/wind.py:362` | `vendor/metrust/src/calc/wind.rs:210` | Kernel uses centered "box" weights `dh = (h[k+1] - h[k-1])/2`; metrust does trapezoidal with interpolated layer endpoints. | O(0.1 m/s) on smooth profiles |
| `bunkers_storm_motion` | `met-cu/python/metcu/kernels/wind.py:586` | `vendor/metrust/src/calc/wind.rs:286` | Kernel uses height-weighted 0-6 km mean wind + simple (top - bottom) bulk shear. metrust uses pressure-weighted mean wind + (mean(5.5-6 km) - mean(0-0.5 km)) shear. | O(m/s) on RM/LM vectors |
| `corfidi_storm_motion` | `met-cu/python/metcu/kernels/wind.py:718` | `vendor/metrust/src/calc/wind.rs:403` | Kernel inherits mean_wind divergence (centered-box vs trapezoidal-with-interp). | tracks `mean_wind` divergence |
| `get_layer` (column) | `met-cu/python/metcu/kernels/wind.py:849` | `vendor/wx-math/src/thermo.rs:1624` | Kernel performs pure level selection inside `[p_top, p_bottom]`. wx-math additionally interpolates new endpoints in log-pressure at the boundaries. Outputs differ at layer edges. | endpoint-only, otherwise exact |
| `shear_vorticity` | `met-cu/python/metcu/kernels/grid.py:406` | `vendor/wx-math/src/dynamics.rs:550` | Direct kernel formula `-(v² dudx + u² dvdy − uv(dvdx + dudy)) / V²` does not algebraically equal `vorticity − curvature_vorticity` (which evaluates to `(v² dvdx − u² dudy + uv(dudx − dvdy)) / V²`). Verified at u=2, v=1, dudx=1, dudy=2, dvdx=3, dvdy=4: kernel → -1.4, CPU → -2.2. The Python comment "Derived from zeta_s = zeta - zeta_c" appears incorrect. | full field-magnitude divergence |
| `smooth_gaussian` | `met-cu/python/metcu/kernels/grid.py:1064` | `vendor/metrust/src/calc/smooth.rs:105` | Kernel applies a 2D Gaussian directly (radius defaults to `ceil(3σ)`); metrust uses 1D-separable Gaussian with `half = ceil(4σ)` and per-pass NaN-skip normalization. Interior agrees by separability, but boundary handling and default radius differ. | structural at edges |
| `smooth_rectangular` | `met-cu/python/metcu/kernels/grid.py:1173` | `vendor/metrust/src/calc/smooth.rs:206` | Kernel uses separate `radius_x`/`radius_y` and averages over the visible window everywhere; metrust uses a single odd `size`, leaves the border `size/2` cells wide unsmoothed, and supports multiple passes via SAT. | structural |
| `smooth_circular` | `met-cu/python/metcu/kernels/grid.py:1224` | `vendor/metrust/src/calc/smooth.rs:342` | Kernel averages over the visible disk using `di² + dj² <= r²`; metrust uses Euclidean `sqrt(di² + dj²) <= r`, leaves the border `ceil(r)` cells wide unsmoothed, and supports `passes`. | structural |
| `lat_lon_grid_deltas` | `met-cu/python/metcu/kernels/grid.py:1660` | `vendor/wx-math/src/gridmath.rs:178` | Kernel uses one-sided forward Haversine (i → i+1) on interior points and `earth_radius = 6_371_229.0 m`. wx-math uses centered (i-1 → i+1)/2 with `EARTH_RADIUS = 6_371_000.0 m`. Both stencil structure and reference radius differ. | ~10 m absolute / ~3.6e-5 relative |

## Per-calc reconciliation plan

### `smooth_n_point`
Choose one weighting. Either rewrite the kernel to use metrust's
MetPy-exact 0.25/0.125/0.0625, or accept met-cu's smoother (0.40/0.10/0.05)
and update metrust to match. Cosmetic difference; pick the one already used
in production rendering.

### `showalter_index`
Determine the dp step size used in metrust's moist-adiabat integrator and
match it in the met-cu RK4 kernel. ~7% is too large to hide.

### `cin`
metrust has a known bug. Two paths: (a) ship met-cu's correct LFC-bounded
CIN as the GPU implementation and document the intentional disagreement
until metrust ships a fix; (b) defer until metrust v0.3.12+ lands the
LFC-bounded version. Project preference is (b) per the "metrust is
authoritative" rule.

### `dcp`
Locate the source of both implementations, confirm the formula difference,
then port whichever matches metrust.
