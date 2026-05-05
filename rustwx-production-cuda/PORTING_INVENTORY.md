# Porting inventory

Source of truth for which calculations are CUDA-ported. Each row lists the
metrust CPU reference, the met-cu kernel it's ported from, and current
status. **Skipped** kernels are tracked in `DIVERGENT_KERNELS.md`.

The full surface is 132 metrust calc functions (141 minus 9 ECAPE). The
table is grouped to match the workspace crates.

## Status legend

- ✅ ported, verified ≤ 1e-10 vs metrust
- 🟡 ported, test pending
- ⬜ pending
- ⏸ deferred (see DIVERGENT_KERNELS.md)
- 🚫 skipped on purpose (ECAPE, pure scalar utility, etc.)

## rustwx-cuda-thermo

| Function | metrust ref | met-cu kernel | Status |
|---|---|---|---|
| potential_temperature | `wx-math/src/thermo.rs:832` | `thermo.py:198` | 🟡 |
| temperature_from_potential_temperature | wx-math/thermo | `thermo.py:212` | ⬜ |
| virtual_temperature | wx-math/thermo | `thermo.py:224` | ⬜ |
| virtual_temperature_from_dewpoint | wx-math/thermo | `thermo.py:253` | ⬜ |
| virtual_potential_temperature | wx-math/thermo | `thermo.py:258` | ⬜ |
| equivalent_potential_temperature | wx-math/thermo | `thermo.py:296` | ⬜ |
| saturation_equivalent_potential_temperature | wx-math/thermo | `thermo.py:322` | ⬜ |
| wet_bulb_potential_temperature | wx-math/thermo | `thermo.py:350` | ⬜ |
| wet_bulb_temperature | wx-math/thermo | `thermo.py:377` | ⬜ |
| saturation_vapor_pressure | wx-math/thermo | `thermo.py:392` | ⬜ |
| vapor_pressure_from_mixing_ratio | wx-math/thermo | `thermo.py:397` | ⬜ |
| vapor_pressure_from_dewpoint | wx-math/thermo | `thermo.py:418` | ⬜ |
| dewpoint | wx-math/thermo | `thermo.py:423` | ⬜ |
| dewpoint_from_relative_humidity | wx-math/thermo | `thermo.py:449` | ⬜ |
| dewpoint_from_specific_humidity | wx-math/thermo | `thermo.py:468` | ⬜ |
| mixing_ratio | wx-math/thermo | `thermo.py:473` | ⬜ |
| saturation_mixing_ratio | wx-math/thermo | `thermo.py:496` | ⬜ |
| mixing_ratio_from_relative_humidity | wx-math/thermo | `thermo.py:513` | ⬜ |
| mixing_ratio_from_specific_humidity | wx-math/thermo | `thermo.py:518` | ⬜ |
| specific_humidity_from_dewpoint | wx-math/thermo | `thermo.py:542` | ⬜ |
| specific_humidity_from_mixing_ratio | wx-math/thermo | `thermo.py:547` | ⬜ |
| relative_humidity_from_dewpoint | wx-math/thermo | `thermo.py:571` | ⬜ |
| relative_humidity_from_mixing_ratio | wx-math/thermo | `thermo.py:588` | ⬜ |
| relative_humidity_from_specific_humidity | wx-math/thermo | `thermo.py:607` | ⬜ |
| density | wx-math/thermo | `thermo.py:612` | ⬜ |
| dry_static_energy | wx-math/thermo | `thermo.py:628` | ⬜ |
| moist_static_energy | wx-math/thermo | `thermo.py:640` | ⬜ |
| exner_function | wx-math/thermo | `thermo.py:652` | ⬜ |
| dry_lapse | wx-math/thermo | `thermo.py:664` | ⬜ |
| height_to_pressure_std | wx-math/atmo | `thermo.py:677` | ⬜ |
| pressure_to_height_std | wx-math/atmo | `thermo.py:694` | ⬜ |
| altimeter_to_sea_level_pressure | wx-math/atmo | `thermo.py:748` | ⬜ |
| altimeter_to_station_pressure | wx-math/atmo | `thermo.py:772` | ⬜ |
| station_to_altimeter_pressure | wx-math/atmo | `thermo.py:788` | ⬜ |
| sigma_to_pressure | wx-math/atmo | `thermo.py:806` | ⬜ |
| geopotential_to_height | wx-math/atmo | `thermo.py:818` | ⬜ |
| height_to_geopotential | wx-math/atmo | `thermo.py:830` | ⬜ |
| scale_height | wx-math/atmo | `thermo.py:842` | ⬜ |
| thickness_hydrostatic | wx-math/thermo | `thermo.py:854` | ⬜ |
| brunt_vaisala_frequency_squared | wx-math/thermo | `thermo.py:890` | ⬜ |
| brunt_vaisala_frequency | wx-math/thermo | `thermo.py:921` | ⬜ |
| brunt_vaisala_period | wx-math/thermo | `thermo.py:926` | ⬜ |
| static_stability | wx-math/thermo | `thermo.py:969` | ⬜ |
| vertical_velocity | wx-math/thermo | `thermo.py:974` | ⬜ |
| vertical_velocity_pressure | wx-math/thermo | `thermo.py:989` | ⬜ |
| montgomery_streamfunction | wx-math/thermo | `thermo.py:1004` | ⬜ |
| heat_index | wx-math/composite | `thermo.py:1016` | ⬜ |
| windchill | wx-math/composite | `thermo.py:1052` | ⬜ |
| apparent_temperature | wx-math/composite | `thermo.py:1107` | ⬜ |
| frost_point | wx-math/thermo | `thermo.py:1126` | ⬜ |
| psychrometric_vapor_pressure | wx-math/thermo | `thermo.py:1144` | ⬜ |
| water_latent_heat_vaporization | wx-math/thermo | `thermo.py:1149` | ⬜ |
| water_latent_heat_sublimation | wx-math/thermo | `thermo.py:1161` | ⬜ |
| water_latent_heat_melting | wx-math/thermo | `thermo.py:1175` | ⬜ |
| moist_air_gas_constant | wx-math/thermo | `thermo.py:1187` | ⬜ |
| moist_air_specific_heat_pressure | wx-math/thermo | `thermo.py:1201` | ⬜ |
| moist_air_poisson_exponent | wx-math/thermo | `thermo.py:1215` | ⬜ |
| moist_lapse | wx-math/thermo | `thermo.py:1270` | ⬜ |
| parcel_profile_with_lcl | wx-math/thermo | `thermo.py:1339` | ⬜ |
| lcl | wx-math/thermo | `thermo.py:1587` | ⬜ |
| lfc | wx-math/thermo | `thermo.py:1651` | ⬜ |
| el | wx-math/thermo | `thermo.py:1710` | ⬜ |
| lifted_index | wx-math/thermo | `thermo.py:1760` | ⬜ |
| precipitable_water | wx-math/thermo | `thermo.py:1786` | ⬜ |
| mixed_layer | wx-math/thermo | `thermo.py:1837` | ⬜ |
| downdraft_cape | wx-math/thermo | `thermo.py:1915` | ⬜ |
| ccl | wx-math/thermo | `thermo.py:1951` | ⬜ |
| cape_cin (column) | wx-math/thermo | `thermo.py:1568` | 🚫 skip — column CAPE is owned by ecape-rs |
| showalter_index | wx-math/thermo | `wind.py:1232` | ⏸ deferred |
| cin (standalone) | wx-math/thermo | `thermo.py:1539` | ⏸ deferred |

## rustwx-cuda-wind

| Function | metrust ref | met-cu kernel | Status |
|---|---|---|---|
| wind_speed | wx-math/dynamics | `wind.py:41` | 🟡 |
| wind_direction | wx-math/dynamics | `wind.py:55` | 🟡 |
| wind_components | wx-math/dynamics | `wind.py:75` | 🟡 |
| coriolis_parameter | wx-math/dynamics | `wind.py:94` | 🟡 |
| angle_to_direction | wx-math/dynamics | `wind.py:109` | ⏸ deferred — CPU returns &str |
| normal_component | wx-math/dynamics | `wind.py:129` | 🟡 |
| tangential_component | wx-math/dynamics | `wind.py:155` | 🟡 |
| friction_velocity | wx-math/dynamics | `wind.py:170` | ⏸ deferred — CPU/CUDA algos differ |
| tke | wx-math/dynamics | `wind.py:214` | 🟡 |
| bulk_shear | wx-math/wind | `wind.py:265` | ⬜ |
| mean_wind | wx-math/wind | `wind.py:362` | ⬜ |
| storm_relative_helicity | wx-math/wind | `wind.py:454` | ⬜ |
| bunkers_storm_motion | wx-math/wind | `wind.py:586` | ⬜ |
| corfidi_storm_motion | wx-math/wind | `wind.py:718` | ⬜ |
| critical_angle | wx-math/dynamics | `wind.py:811` | ⏸ deferred — sign convention mismatch with metrust scalar form |
| get_layer | wx-math/wind | `wind.py:849` | ⬜ |
| temperature_advection | wx-math/wind | n/a (port from grid.py advection) | ⬜ |
| gradient_richardson_number | wx-math/wind | `wind.py:2028` | ⬜ |
| compute_lapse_rate | wx-math/thermo | `wind.py:1838` | ⬜ |
| dendritic_growth_zone | wx-math/wind | `wind.py:1749` | ⬜ |
| warm_nose_check | wx-math/wind | `wind.py:1674` | ⬜ |
| freezing_rain_composite | wx-math/wind | `wind.py:1598` | ⬜ |

## rustwx-cuda-grid

| Function | metrust ref | met-cu kernel | Status |
|---|---|---|---|
| vorticity | wx-math/dynamics | `grid.py:128` | 🟡 |
| divergence | wx-math/dynamics | `grid.py:164` | 🟡 |
| absolute_vorticity | wx-math/dynamics | `grid.py:201` | 🟡 |
| shearing_deformation | wx-math/dynamics | `grid.py:237` | 🟡 |
| stretching_deformation | wx-math/dynamics | `grid.py:273` | 🟡 |
| total_deformation | wx-math/dynamics | `grid.py:313` | 🟡 |
| curvature_vorticity | wx-math/dynamics | `grid.py:359` | ⬜ |
| shear_vorticity | wx-math/dynamics | `grid.py:406` | ⬜ |
| first_derivative_x | wx-math/dynamics | `grid.py:437` | 🟡 |
| first_derivative_y | wx-math/dynamics | `grid.py:468` | 🟡 |
| second_derivative_x | wx-math/dynamics | `grid.py:499` | ⬜ |
| second_derivative_y | wx-math/dynamics | `grid.py:530` | ⬜ |
| laplacian | wx-math/dynamics | `grid.py:562` | 🟡 |
| gradient | wx-math/dynamics | `grid.py:597` | 🟡 |
| advection | wx-math/dynamics | `grid.py:635` | 🟡 |
| frontogenesis | wx-math/dynamics | `grid.py:696` | 🟡 |
| interpolate_vertical | wx-math/regrid | (custom — slab interp) | 🟡 |
| q_vector | wx-math/kinematics | `grid.py:749` | ⬜ |
| geostrophic_wind | wx-math/kinematics | `grid.py:794` | ⬜ |
| ageostrophic_wind | wx-math/kinematics | `grid.py:841` | ⬜ |
| potential_vorticity_baroclinic | wx-math/kinematics | `grid.py:901` | ⬜ |
| potential_vorticity_barotropic | wx-math/kinematics | `grid.py:958` | ⬜ |
| inertial_advective_wind | wx-math/kinematics | `grid.py:1011` | ⬜ |
| smooth_gaussian | wx-math/smooth | `grid.py:1064` | ⬜ |
| smooth_rectangular | wx-math/smooth | `grid.py:1173` | ⬜ |
| smooth_circular | wx-math/smooth | `grid.py:1224` | ⬜ |
| smooth_window | wx-math/smooth | `grid.py:1275` | ⬜ |
| interpolate_1d | wx-math/interp | `grid.py:1351` | ⬜ |
| log_interpolate_1d | wx-math/interp | `grid.py:1427` | ⬜ |
| lat_lon_grid_deltas | wx-math/grid | `grid.py:1660` | ⬜ |
| composite_reflectivity | wx-math/grid | `grid.py:1715` | ⬜ |
| mean_pressure_weighted | wx-math/grid | `grid.py:1777` | ⬜ |
| smooth_n_point | wx-math/smooth | `grid.py:1119` | ⏸ deferred |

## rustwx-cuda-severe

| Function | metrust ref | met-cu kernel | Status |
|---|---|---|---|
| stp (significant_tornado_parameter) | wx-math/composite | `wind.py:930` | ⬜ |
| scp (supercell_composite_parameter) | wx-math/composite | `wind.py:974` | ⬜ |
| ship | wx-math/composite | `wind.py:1001` | ⬜ |
| ehi | wx-math/composite | `wind.py:1031` | ⬜ |
| brn | wx-math/composite | `wind.py:1071` | ⬜ |
| k_index | wx-math/composite | `wind.py:1089` | ⬜ |
| total_totals | wx-math/composite | `wind.py:1106` | ⬜ |
| cross_totals | wx-math/composite | `wind.py:1120` | ⬜ |
| vertical_totals | wx-math/composite | `wind.py:1134` | ⬜ |
| sweat_index | wx-math/composite | `wind.py:1148` | ⬜ |
| boyden_index | wx-math/composite | `wind.py:1348` | ⬜ |
| gdi | wx-math/composite | `wind.py:1369` | ⬜ |
| ffwi | wx-math/composite | `wind.py:1435` | ⬜ |
| haines_index | wx-math/composite | `wind.py:1482` | ⬜ |
| hot_dry_windy | wx-math/composite | `wind.py:1516` | ⬜ |
| sig_tor (alt) | wx-math/composite | `wind.py:1555` | ⬜ |
| dcp | wx-math/composite | TBD | ⏸ deferred |
