//! Projection math and metadata adapters for projected map rendering.

use crate::overlay::MapExtent;
use rustwx_core as core;
use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

const R_EARTH: f64 = 6_370_000.0;
const DEG2RAD: f64 = PI / 180.0;
const RAD2DEG: f64 = 180.0 / PI;
const GEOGRAPHIC_INFERENCE_MIN_LAT_SPAN_DEG: f64 = 100.0;
const GEOGRAPHIC_INFERENCE_MIN_LON_SPAN_DEG: f64 = 300.0;

/// Lightweight, render-local projection metadata.
///
/// The shape mirrors `rustwx_core::GridProjection` so callers can hand native
/// grid metadata through without coupling the render crate directly to parser
/// internals. When projection metadata is unavailable, callers can still omit
/// this and let the projected-map builder fall back to an inferred Lambert
/// conformal setup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectionSpec {
    Geographic,
    LambertConformal {
        standard_parallel_1_deg: f64,
        standard_parallel_2_deg: f64,
        central_meridian_deg: f64,
    },
    PolarStereographic {
        true_latitude_deg: f64,
        central_meridian_deg: f64,
        south_pole_on_projection_plane: bool,
    },
    Mercator {
        latitude_of_true_scale_deg: f64,
        central_meridian_deg: f64,
    },
    Other {
        template: u16,
    },
}

impl ProjectionSpec {
    pub fn is_projected(&self) -> bool {
        !matches!(self, Self::Geographic)
    }

    /// Approximate fallback for legacy callers that only have a lat/lon mesh.
    ///
    /// This intentionally picks a region-centered Lambert conformal setup for
    /// midlatitude weather domains. If upstream native projection metadata is
    /// available, callers should prefer passing it explicitly instead of using
    /// this inference path.
    pub fn infer_from_latlon_grid(lat_deg: &[f32], lon_deg: &[f32]) -> Option<Self> {
        let stats = LatLonStats::from_grid(lat_deg, lon_deg)?;
        let lat_span = (stats.max_lat - stats.min_lat).abs();
        if lat_span >= GEOGRAPHIC_INFERENCE_MIN_LAT_SPAN_DEG
            && stats.lon_span >= GEOGRAPHIC_INFERENCE_MIN_LON_SPAN_DEG
        {
            return Some(Self::Geographic);
        }
        let (sp1, sp2) = inferred_standard_parallels(stats.min_lat, stats.max_lat, lat_span);
        Some(Self::LambertConformal {
            standard_parallel_1_deg: sp1,
            standard_parallel_2_deg: sp2,
            central_meridian_deg: stats.center_lon,
        })
    }

    pub(crate) fn build_projector(
        &self,
        reference_latitude_deg: Option<f64>,
        reference_longitude_deg: Option<f64>,
        lat_deg: &[f32],
        lon_deg: &[f32],
    ) -> Result<ProjectionProjector, &'static str> {
        match *self {
            Self::Geographic => {
                let center_lon = reference_longitude_deg
                    .map(normalize_longitude_deg)
                    .or_else(|| circular_mean_longitude_deg(lon_deg))
                    .ok_or("projection requires at least one finite longitude")?;
                Ok(ProjectionProjector::Geographic(GeographicProjection {
                    central_meridian_deg: center_lon,
                }))
            }
            Self::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            } => {
                let ref_lat = reference_latitude_deg
                    .or_else(|| latitude_midpoint_deg(lat_deg))
                    .unwrap_or((standard_parallel_1_deg + standard_parallel_2_deg) / 2.0);
                Ok(ProjectionProjector::LambertConformal(
                    LambertConformal::new(
                        standard_parallel_1_deg,
                        standard_parallel_2_deg,
                        central_meridian_deg,
                        stabilize_reference_latitude(ref_lat),
                    ),
                ))
            }
            Self::PolarStereographic {
                true_latitude_deg,
                central_meridian_deg,
                south_pole_on_projection_plane,
            } => Ok(ProjectionProjector::PolarStereographic(
                PolarStereographic::new(
                    true_latitude_deg,
                    central_meridian_deg,
                    south_pole_on_projection_plane,
                ),
            )),
            Self::Mercator {
                latitude_of_true_scale_deg,
                central_meridian_deg,
            } => Ok(ProjectionProjector::Mercator(MercatorProjection::new(
                latitude_of_true_scale_deg,
                central_meridian_deg,
            ))),
            Self::Other { .. } => Err("projection template is not supported by rustwx-render"),
        }
    }
}

impl From<core::GridProjection> for ProjectionSpec {
    fn from(value: core::GridProjection) -> Self {
        match value {
            core::GridProjection::Geographic => Self::Geographic,
            core::GridProjection::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            } => Self::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            },
            core::GridProjection::PolarStereographic {
                true_latitude_deg,
                central_meridian_deg,
                south_pole_on_projection_plane,
            } => Self::PolarStereographic {
                true_latitude_deg,
                central_meridian_deg,
                south_pole_on_projection_plane,
            },
            core::GridProjection::Mercator {
                latitude_of_true_scale_deg,
                central_meridian_deg,
            } => Self::Mercator {
                latitude_of_true_scale_deg,
                central_meridian_deg,
            },
            core::GridProjection::Other { template } => Self::Other { template },
        }
    }
}

impl From<ProjectionSpec> for core::GridProjection {
    fn from(value: ProjectionSpec) -> Self {
        match value {
            ProjectionSpec::Geographic => Self::Geographic,
            ProjectionSpec::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            } => Self::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            },
            ProjectionSpec::PolarStereographic {
                true_latitude_deg,
                central_meridian_deg,
                south_pole_on_projection_plane,
            } => Self::PolarStereographic {
                true_latitude_deg,
                central_meridian_deg,
                south_pole_on_projection_plane,
            },
            ProjectionSpec::Mercator {
                latitude_of_true_scale_deg,
                central_meridian_deg,
            } => Self::Mercator {
                latitude_of_true_scale_deg,
                central_meridian_deg,
            },
            ProjectionSpec::Other { template } => Self::Other { template },
        }
    }
}

/// Lambert conformal conic projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LambertConformal {
    n: f64,
    f: f64,
    rho0: f64,
    lambda0: f64,
    truelat1_deg: f64,
    truelat2_deg: f64,
    stand_lon_deg: f64,
    ref_lat_deg: f64,
}

impl LambertConformal {
    /// Create from degrees.
    pub fn new(truelat1_deg: f64, truelat2_deg: f64, stand_lon_deg: f64, ref_lat_deg: f64) -> Self {
        let phi1 = stabilize_reference_latitude(truelat1_deg) * DEG2RAD;
        let phi2 = stabilize_reference_latitude(truelat2_deg) * DEG2RAD;
        let phi0 = stabilize_reference_latitude(ref_lat_deg) * DEG2RAD;
        let lambda0 = stand_lon_deg * DEG2RAD;

        let mut n = if (truelat1_deg - truelat2_deg).abs() < 1e-10 {
            phi1.sin()
        } else {
            let num = (phi1.cos()).ln() - (phi2.cos()).ln();
            let den = ((PI / 4.0 + phi2 / 2.0).tan()).ln() - ((PI / 4.0 + phi1 / 2.0).tan()).ln();
            num / den
        };
        if n.abs() < 1e-8 {
            let fallback = if phi0.abs() >= 1e-8 {
                phi0
            } else if phi1.abs() >= 1e-8 {
                phi1
            } else {
                10.0 * DEG2RAD
            };
            n = fallback.sin();
        }

        let f = phi1.cos() * (PI / 4.0 + phi1 / 2.0).tan().powf(n) / n;
        let rho0 = R_EARTH * f / (PI / 4.0 + phi0 / 2.0).tan().powf(n);

        Self {
            n,
            f,
            rho0,
            lambda0,
            truelat1_deg,
            truelat2_deg,
            stand_lon_deg,
            ref_lat_deg,
        }
    }

    pub fn project(&self, lat: f64, lon: f64) -> (f64, f64) {
        let phi = stabilize_latitude(lat) * DEG2RAD;
        let delta_lon = normalize_longitude_deg(lon - self.stand_lon_deg) * DEG2RAD;

        let rho = R_EARTH * self.f / (PI / 4.0 + phi / 2.0).tan().powf(self.n);
        let theta = self.n * delta_lon;

        let x = rho * theta.sin();
        let y = self.rho0 - rho * theta.cos();
        (x, y)
    }

    pub fn spec(self) -> ProjectionSpec {
        ProjectionSpec::LambertConformal {
            standard_parallel_1_deg: self.truelat1_deg,
            standard_parallel_2_deg: self.truelat2_deg,
            central_meridian_deg: self.stand_lon_deg,
        }
    }

    pub fn reference_latitude_deg(self) -> f64 {
        self.ref_lat_deg
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GeographicProjection {
    central_meridian_deg: f64,
}

impl GeographicProjection {
    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        (
            normalize_longitude_deg(lon - self.central_meridian_deg),
            stabilize_latitude(lat),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PolarStereographic {
    central_meridian_deg: f64,
    south_pole_on_projection_plane: bool,
    k: f64,
}

impl PolarStereographic {
    fn new(
        true_latitude_deg: f64,
        central_meridian_deg: f64,
        south_pole_on_projection_plane: bool,
    ) -> Self {
        let lat_ts = stabilize_reference_latitude(true_latitude_deg) * DEG2RAD;
        Self {
            central_meridian_deg,
            south_pole_on_projection_plane,
            k: (1.0 + lat_ts.sin()) / 2.0,
        }
    }

    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        let phi = stabilize_latitude(lat) * DEG2RAD;
        let theta = normalize_longitude_deg(lon - self.central_meridian_deg) * DEG2RAD;
        if self.south_pole_on_projection_plane {
            let rho = 2.0 * R_EARTH * self.k * (PI / 4.0 + phi / 2.0).tan();
            (rho * theta.sin(), rho * theta.cos())
        } else {
            let rho = 2.0 * R_EARTH * self.k * (PI / 4.0 - phi / 2.0).tan();
            (rho * theta.sin(), -rho * theta.cos())
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MercatorProjection {
    central_meridian_deg: f64,
    scale: f64,
}

impl MercatorProjection {
    fn new(latitude_of_true_scale_deg: f64, central_meridian_deg: f64) -> Self {
        Self {
            central_meridian_deg,
            scale: (stabilize_reference_latitude(latitude_of_true_scale_deg) * DEG2RAD)
                .cos()
                .max(1.0e-6),
        }
    }

    fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        let phi = stabilize_latitude(lat) * DEG2RAD;
        let lambda = normalize_longitude_deg(lon - self.central_meridian_deg) * DEG2RAD;
        let x = R_EARTH * self.scale * lambda;
        let y = R_EARTH * self.scale * ((PI / 4.0 + phi / 2.0).tan()).ln();
        (x, y)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ProjectionProjector {
    Geographic(GeographicProjection),
    LambertConformal(LambertConformal),
    PolarStereographic(PolarStereographic),
    Mercator(MercatorProjection),
}

impl ProjectionProjector {
    pub(crate) fn project(self, lat: f64, lon: f64) -> (f64, f64) {
        match self {
            Self::Geographic(projector) => projector.project(lat, lon),
            Self::LambertConformal(projector) => projector.project(lat, lon),
            Self::PolarStereographic(projector) => projector.project(lat, lon),
            Self::Mercator(projector) => projector.project(lat, lon),
        }
    }
}

impl MapExtent {
    pub fn from_wrf(
        proj: &LambertConformal,
        cen_lat: f64,
        cen_lon: f64,
        nx: usize,
        ny: usize,
        dx: f64,
        dy: f64,
    ) -> Self {
        let (xc, yc) = proj.project(cen_lat, cen_lon);
        Self {
            x_min: xc - dx * (nx as f64 - 1.0) / 2.0,
            x_max: xc + dx * (nx as f64 - 1.0) / 2.0,
            y_min: yc - dy * (ny as f64 - 1.0) / 2.0,
            y_max: yc + dy * (ny as f64 - 1.0) / 2.0,
        }
    }

    pub fn from_bounds(x_min: f64, x_max: f64, y_min: f64, y_max: f64, target_ratio: f64) -> Self {
        let data_width = x_max - x_min;
        let data_height = y_max - y_min;
        let safe_ratio = target_ratio.max(1.0e-6);
        let data_ratio = data_width / data_height.max(1e-12);

        if data_ratio > safe_ratio {
            let new_height = data_width / safe_ratio;
            let pad_y = (new_height - data_height) / 2.0;
            Self {
                x_min,
                x_max,
                y_min: y_min - pad_y,
                y_max: y_max + pad_y,
            }
        } else {
            let new_width = data_height * safe_ratio;
            let pad_x = (new_width - data_width) / 2.0;
            Self {
                x_min: x_min - pad_x,
                x_max: x_max + pad_x,
                y_min,
                y_max,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LatLonStats {
    min_lat: f64,
    max_lat: f64,
    center_lon: f64,
    lon_span: f64,
}

impl LatLonStats {
    fn from_grid(lat_deg: &[f32], lon_deg: &[f32]) -> Option<Self> {
        let mut min_lat = f64::INFINITY;
        let mut max_lat = f64::NEG_INFINITY;

        for &lat in lat_deg {
            let lat = lat as f64;
            if !lat.is_finite() {
                continue;
            }
            min_lat = min_lat.min(lat);
            max_lat = max_lat.max(lat);
        }

        if !min_lat.is_finite() || !max_lat.is_finite() {
            return None;
        }

        Some(Self {
            min_lat,
            max_lat,
            center_lon: circular_mean_longitude_deg(lon_deg)?,
            lon_span: minimal_longitude_span_deg(lon_deg)?,
        })
    }
}

fn minimal_longitude_span_deg(lon_deg: &[f32]) -> Option<f64> {
    let mut lon: Vec<f64> = lon_deg
        .iter()
        .filter_map(|&value| {
            let value = value as f64;
            value
                .is_finite()
                .then_some(normalize_longitude_positive_deg(value))
        })
        .collect();

    if lon.is_empty() {
        return None;
    }
    lon.sort_by(f64::total_cmp);
    lon.dedup_by(|a, b| (*a - *b).abs() < 1.0e-9);
    if lon.len() == 1 {
        return Some(0.0);
    }

    let mut max_gap: f64 = 0.0;
    for pair in lon.windows(2) {
        max_gap = max_gap.max(pair[1] - pair[0]);
    }
    max_gap = max_gap.max(lon[0] + 360.0 - lon[lon.len() - 1]);
    Some((360.0 - max_gap).clamp(0.0, 360.0))
}

fn inferred_standard_parallels(min_lat: f64, max_lat: f64, lat_span: f64) -> (f64, f64) {
    if min_lat < 0.0 && max_lat > 0.0 {
        let dominant = if max_lat.abs() >= min_lat.abs() {
            max_lat
        } else {
            min_lat
        };
        let tangent = stabilize_reference_latitude((dominant * 0.75).max(5.0 * dominant.signum()));
        return (tangent, tangent);
    }

    if lat_span < 6.0 {
        let tangent = stabilize_reference_latitude((min_lat + max_lat) / 2.0);
        return (tangent, tangent);
    }

    let inset = (lat_span / 6.0).clamp(2.0, 12.0);
    let sp1 = stabilize_reference_latitude(min_lat + inset);
    let sp2 = stabilize_reference_latitude(max_lat - inset);
    if (sp2 - sp1).abs() < 0.25 {
        (sp1, sp1)
    } else {
        (sp1, sp2)
    }
}

fn latitude_midpoint_deg(lat_deg: &[f32]) -> Option<f64> {
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    for &lat in lat_deg {
        let lat = lat as f64;
        if !lat.is_finite() {
            continue;
        }
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
    }
    (min_lat.is_finite() && max_lat.is_finite()).then_some((min_lat + max_lat) / 2.0)
}

fn circular_mean_longitude_deg(lon_deg: &[f32]) -> Option<f64> {
    let mut sin_sum = 0.0;
    let mut cos_sum = 0.0;
    let mut count = 0usize;
    for &lon in lon_deg {
        let lon = lon as f64;
        if !lon.is_finite() {
            continue;
        }
        let lambda = lon * DEG2RAD;
        sin_sum += lambda.sin();
        cos_sum += lambda.cos();
        count += 1;
    }
    if count == 0 {
        return None;
    }
    if sin_sum.abs() < 1.0e-9 && cos_sum.abs() < 1.0e-9 {
        return Some(0.0);
    }
    Some(normalize_longitude_deg(sin_sum.atan2(cos_sum) * RAD2DEG))
}

fn stabilize_latitude(lat_deg: f64) -> f64 {
    lat_deg.clamp(-89.999, 89.999)
}

fn stabilize_reference_latitude(lat_deg: f64) -> f64 {
    let clamped = lat_deg.clamp(-85.0, 85.0);
    if clamped.abs() < 1.0 {
        10.0_f64.copysign(if clamped < 0.0 { -1.0 } else { 1.0 })
    } else {
        clamped
    }
}

fn normalize_longitude_deg(lon_deg: f64) -> f64 {
    let mut lon = lon_deg % 360.0;
    if lon > 180.0 {
        lon -= 360.0;
    } else if lon <= -180.0 {
        lon += 360.0;
    }
    lon
}

fn normalize_longitude_positive_deg(lon_deg: f64) -> f64 {
    let mut lon = lon_deg % 360.0;
    if lon < 0.0 {
        lon += 360.0;
    }
    lon
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_spec_round_trips_through_core_grid_projection() {
        let spec = ProjectionSpec::Mercator {
            latitude_of_true_scale_deg: 25.0,
            central_meridian_deg: -90.0,
        };
        let core: core::GridProjection = spec.clone().into();
        let round_trip = ProjectionSpec::from(core);
        assert_eq!(round_trip, spec);
    }

    #[test]
    fn inferred_projection_uses_region_centered_lambert() {
        let lat = vec![25.0, 25.0, 50.0, 50.0];
        let lon = vec![-124.0, -66.0, -124.0, -66.0];
        let inferred = ProjectionSpec::infer_from_latlon_grid(&lat, &lon)
            .expect("finite grid should infer a fallback projection");

        match inferred {
            ProjectionSpec::LambertConformal {
                standard_parallel_1_deg,
                standard_parallel_2_deg,
                central_meridian_deg,
            } => {
                assert!(standard_parallel_1_deg > 25.0);
                assert!(standard_parallel_2_deg < 50.0);
                assert!(central_meridian_deg < -90.0);
                assert!(central_meridian_deg > -100.0);
            }
            other => panic!("expected inferred Lambert conformal, got {other:?}"),
        }
    }

    #[test]
    fn inferred_projection_uses_geographic_for_global_latlon_grid() {
        let mut lat = Vec::new();
        let mut lon = Vec::new();
        for row_lat in [-90.0f32, 0.0, 90.0] {
            for col_lon in (0..360).step_by(30) {
                lat.push(row_lat);
                lon.push(col_lon as f32);
            }
        }

        let inferred = ProjectionSpec::infer_from_latlon_grid(&lat, &lon)
            .expect("finite global grid should infer a fallback projection");
        assert_eq!(inferred, ProjectionSpec::Geographic);
    }

    #[test]
    fn minimal_longitude_span_handles_antimeridian_domains() {
        let lon = vec![176.0, 179.0, -179.0, -178.0];
        let span = minimal_longitude_span_deg(&lon).expect("finite span");
        assert!(span < 7.0, "expected a small antimeridian span, got {span}");
    }

    #[test]
    fn geographic_projector_centers_longitudes() {
        let spec = ProjectionSpec::Geographic;
        let projector = spec
            .build_projector(None, None, &[40.0, 41.0], &[-110.0, -90.0])
            .expect("geographic projector");
        let (x_west, y) = projector.project(40.0, -110.0);
        let (x_east, _) = projector.project(40.0, -90.0);
        assert!(x_west < 0.0);
        assert!(x_east > 0.0);
        assert_eq!(y, 40.0);
    }

    #[test]
    fn projected_families_return_finite_points() {
        let lat = [40.0f32, 42.0];
        let lon = [-110.0f32, -100.0];
        let specs = [
            ProjectionSpec::LambertConformal {
                standard_parallel_1_deg: 33.0,
                standard_parallel_2_deg: 45.0,
                central_meridian_deg: -97.0,
            },
            ProjectionSpec::Mercator {
                latitude_of_true_scale_deg: 25.0,
                central_meridian_deg: -95.0,
            },
            ProjectionSpec::PolarStereographic {
                true_latitude_deg: 60.0,
                central_meridian_deg: -105.0,
                south_pole_on_projection_plane: false,
            },
        ];

        for spec in specs {
            let projector = spec
                .build_projector(None, None, &lat, &lon)
                .expect("supported projection");
            let (x, y) = projector.project(40.0, -105.0);
            assert!(x.is_finite());
            assert!(y.is_finite());
        }
    }

    #[test]
    fn unsupported_projection_template_is_rejected() {
        let spec = ProjectionSpec::Other { template: 1234 };
        let error = spec
            .build_projector(None, None, &[40.0], &[-100.0])
            .unwrap_err();
        assert!(error.contains("not supported"));
    }
}
