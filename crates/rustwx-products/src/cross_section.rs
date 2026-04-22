use std::cmp::Ordering;
use std::time::Instant;

use rustwx_calc::{
    compute_relative_humidity_from_pressure_temperature_and_mixing_ratio,
    compute_theta_e_from_pressure_temperature_and_mixing_ratio,
};
use rustwx_core::{CycleSpec, ModelId, SourceId};
use rustwx_cross_section::{
    CrossSectionProduct, CrossSectionStyle, DecomposedWindGrid, GeoPoint, HorizontalInterpolation,
    ScalarSection, SectionLayout, SectionMetadata, TerrainProfile, VerticalAxis, WindOverlayBundle,
    WindOverlayStyle, decompose_wind_grid,
};

use crate::gridded::{LoadedModelTimestep, PressureFields, SurfaceFields};

const SECTION_CANDIDATE_PADDING_DEG: f64 = 1.5;
const INTERPOLATED_NEIGHBOR_COUNT: usize = 4;
const MS_TO_KT: f64 = 1.943_844_492_440_604_8;

pub const SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS: [CrossSectionProduct; 4] = [
    CrossSectionProduct::Temperature,
    CrossSectionProduct::RelativeHumidity,
    CrossSectionProduct::ThetaE,
    CrossSectionProduct::WindSpeed,
];

#[derive(Debug, Clone)]
pub struct PressureCrossSectionArtifact {
    pub section: ScalarSection,
    pub style: CrossSectionStyle,
    pub wind_overlay: WindOverlayBundle,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PressureCrossSectionBuildTiming {
    pub stencil_build_ms: u128,
    pub terrain_profile_ms: u128,
    pub pressure_sampling_ms: u128,
    pub product_compute_ms: u128,
    pub metadata_ms: u128,
    pub section_assembly_ms: u128,
    pub wind_overlay_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone)]
pub struct ProfiledPressureCrossSectionArtifact {
    pub artifact: PressureCrossSectionArtifact,
    pub timing: PressureCrossSectionBuildTiming,
}

pub fn supports_pressure_cross_section_product(product: CrossSectionProduct) -> bool {
    SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS.contains(&product)
}

pub fn build_pressure_cross_section(
    loaded: &LoadedModelTimestep,
    layout: &SectionLayout,
    product: CrossSectionProduct,
) -> Result<PressureCrossSectionArtifact, Box<dyn std::error::Error>> {
    build_pressure_cross_section_profiled(loaded, layout, product).map(|profiled| profiled.artifact)
}

pub fn build_pressure_cross_section_profiled(
    loaded: &LoadedModelTimestep,
    layout: &SectionLayout,
    product: CrossSectionProduct,
) -> Result<ProfiledPressureCrossSectionArtifact, Box<dyn std::error::Error>> {
    build_pressure_cross_section_from_parts_profiled(
        &loaded.surface_decode.value,
        &loaded.pressure_decode.value,
        loaded.model,
        loaded.latest.source,
        &loaded.latest.cycle,
        loaded.surface_file.request.request.forecast_hour,
        layout,
        product,
    )
}

fn build_pressure_cross_section_from_parts_profiled(
    surface: &SurfaceFields,
    pressure: &PressureFields,
    model: ModelId,
    source: SourceId,
    cycle: &CycleSpec,
    forecast_hour: u16,
    layout: &SectionLayout,
    product: CrossSectionProduct,
) -> Result<ProfiledPressureCrossSectionArtifact, Box<dyn std::error::Error>> {
    let total_start = Instant::now();
    if !supports_pressure_cross_section_product(product) {
        return Err(format!(
            "cross-section product '{}' is not yet wired for gridded pressure sections",
            product.slug()
        )
        .into());
    }

    let sampled_points = layout.sampled_path.points();
    let sampled_distances = layout.sampled_path.distances_km();
    let sampled_bearings = layout.sampled_path.bearings_deg();
    let stencil_build_start = Instant::now();
    let stencils = build_sample_stencils(surface, &sampled_points, layout.interpolation);
    let stencil_build_ms = stencil_build_start.elapsed().as_millis();
    let nxy = surface.nx * surface.ny;
    let n_points = sampled_points.len();
    let n_levels = pressure.pressure_levels_hpa.len();

    let terrain_profile_start = Instant::now();
    let surface_pressure_hpa = stencils
        .iter()
        .map(|stencil| sample_weighted_2d(&surface.psfc_pa, stencil) / 100.0)
        .collect::<Vec<_>>();
    let surface_height_m = stencils
        .iter()
        .map(|stencil| sample_weighted_2d(&surface.orog_m, stencil))
        .collect::<Vec<_>>();

    let terrain = TerrainProfile::new(sampled_distances.clone())?
        .with_surface_pressure_hpa(surface_pressure_hpa)?
        .with_surface_height_m(surface_height_m)?;
    let terrain_profile_ms = terrain_profile_start.elapsed().as_millis();

    let mut temperature_c = Vec::with_capacity(n_levels * n_points);
    let mut qvapor_kgkg = Vec::with_capacity(n_levels * n_points);
    let mut u_ms = Vec::with_capacity(n_levels * n_points);
    let mut v_ms = Vec::with_capacity(n_levels * n_points);
    let mut pressure_hpa = Vec::with_capacity(n_levels * n_points);
    let pressure_sampling_start = Instant::now();
    for (level_index, level_hpa) in pressure.pressure_levels_hpa.iter().copied().enumerate() {
        let level_offset = level_index * nxy;
        for stencil in &stencils {
            temperature_c.push(sample_weighted_level(
                &pressure.temperature_c_3d,
                level_offset,
                stencil,
            ));
            qvapor_kgkg.push(sample_weighted_level(
                &pressure.qvapor_kgkg_3d,
                level_offset,
                stencil,
            ));
            u_ms.push(sample_weighted_level(
                &pressure.u_ms_3d,
                level_offset,
                stencil,
            ));
            v_ms.push(sample_weighted_level(
                &pressure.v_ms_3d,
                level_offset,
                stencil,
            ));
            pressure_hpa.push(level_hpa);
        }
    }
    let pressure_sampling_ms = pressure_sampling_start.elapsed().as_millis();

    let product_compute_start = Instant::now();
    let section_values = build_product_values(
        product,
        &pressure_hpa,
        &temperature_c,
        &qvapor_kgkg,
        &u_ms,
        &v_ms,
    )?
    .into_iter()
    .map(|value| value as f32)
    .collect::<Vec<_>>();
    let product_compute_ms = product_compute_start.elapsed().as_millis();

    let metadata_start = Instant::now();
    let metadata = build_section_metadata(layout, model, source, cycle, forecast_hour, product);
    let metadata_ms = metadata_start.elapsed().as_millis();
    let section_assembly_start = Instant::now();
    let section = ScalarSection::new(
        sampled_distances.clone(),
        VerticalAxis::pressure_hpa(pressure.pressure_levels_hpa.clone())?,
        section_values,
    )?
    .with_metadata(metadata)
    .with_terrain(terrain)?;
    let section_assembly_ms = section_assembly_start.elapsed().as_millis();

    let wind_overlay_start = Instant::now();
    let wind_overlay = WindOverlayBundle::new(
        build_wind_grid(&u_ms, &v_ms, n_levels, n_points, &sampled_bearings)?,
        WindOverlayStyle::default(),
    )
    .with_label("Section Relative Wind");
    let wind_overlay_ms = wind_overlay_start.elapsed().as_millis();

    Ok(ProfiledPressureCrossSectionArtifact {
        artifact: PressureCrossSectionArtifact {
            section,
            style: CrossSectionStyle::new(product),
            wind_overlay,
        },
        timing: PressureCrossSectionBuildTiming {
            stencil_build_ms,
            terrain_profile_ms,
            pressure_sampling_ms,
            product_compute_ms,
            metadata_ms,
            section_assembly_ms,
            wind_overlay_ms,
            total_ms: total_start.elapsed().as_millis(),
        },
    })
}

fn build_product_values(
    product: CrossSectionProduct,
    pressure_hpa: &[f64],
    temperature_c: &[f64],
    qvapor_kgkg: &[f64],
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    match product {
        CrossSectionProduct::Temperature => Ok(temperature_c.to_vec()),
        CrossSectionProduct::RelativeHumidity => Ok(
            compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
                pressure_hpa,
                temperature_c,
                qvapor_kgkg,
            )?,
        ),
        CrossSectionProduct::ThetaE => {
            Ok(compute_theta_e_from_pressure_temperature_and_mixing_ratio(
                pressure_hpa,
                temperature_c,
                qvapor_kgkg,
            )?)
        }
        CrossSectionProduct::WindSpeed => Ok(u_ms
            .iter()
            .zip(v_ms.iter())
            .map(|(&u_ms, &v_ms)| ((u_ms * u_ms + v_ms * v_ms).sqrt()) * MS_TO_KT)
            .collect()),
        _ => Err(format!(
            "cross-section product '{}' is not supported by the gridded pressure builder",
            product.slug()
        )
        .into()),
    }
}

fn build_section_metadata(
    layout: &SectionLayout,
    model: ModelId,
    source: SourceId,
    cycle: &CycleSpec,
    forecast_hour: u16,
    product: CrossSectionProduct,
) -> SectionMetadata {
    let mut metadata = layout.metadata.clone();
    metadata.title.get_or_insert_with(|| {
        format!(
            "{} {} Cross Section",
            model.as_str().to_ascii_uppercase(),
            product.display_name()
        )
    });
    metadata
        .field_name
        .get_or_insert_with(|| product.slug().to_string());
    metadata
        .field_units
        .get_or_insert_with(|| product.units().to_string());
    metadata
        .source
        .get_or_insert_with(|| source.as_str().to_string());
    metadata.valid_label.get_or_insert_with(|| {
        format!(
            "{} {:02}Z F{:03}",
            cycle.date_yyyymmdd, cycle.hour_utc, forecast_hour
        )
    });
    metadata
        .attributes
        .entry("product_key".to_string())
        .or_insert_with(|| product.slug().to_string());
    metadata
        .attributes
        .entry("render_style".to_string())
        .or_insert_with(|| product.style_key().to_string());
    metadata
        .attributes
        .entry("model".to_string())
        .or_insert_with(|| model.as_str().to_ascii_uppercase());
    metadata
}

fn build_wind_grid(
    u_ms: &[f64],
    v_ms: &[f64],
    n_levels: usize,
    n_points: usize,
    sampled_bearings: &[f64],
) -> Result<DecomposedWindGrid, Box<dyn std::error::Error>> {
    let u_ms = u_ms.iter().map(|&value| value as f32).collect::<Vec<_>>();
    let v_ms = v_ms.iter().map(|&value| value as f32).collect::<Vec<_>>();
    Ok(decompose_wind_grid(
        &u_ms,
        &v_ms,
        n_levels,
        n_points,
        sampled_bearings,
    )?)
}

#[derive(Debug, Clone)]
struct SampleStencil {
    indices: Vec<usize>,
    weights: Vec<f64>,
}

fn build_sample_stencils(
    surface: &SurfaceFields,
    sampled_points: &[GeoPoint],
    interpolation: HorizontalInterpolation,
) -> Vec<SampleStencil> {
    let candidates = candidate_indices(surface, sampled_points, SECTION_CANDIDATE_PADDING_DEG);
    sampled_points
        .iter()
        .map(|&point| sample_stencil_for_point(surface, &candidates, point, interpolation))
        .collect()
}

fn sample_stencil_for_point(
    surface: &SurfaceFields,
    candidates: &[usize],
    point: GeoPoint,
    interpolation: HorizontalInterpolation,
) -> SampleStencil {
    let mut nearest = candidates
        .iter()
        .map(|&idx| (idx, geographic_distance_score(surface, idx, point)))
        .collect::<Vec<_>>();
    nearest.sort_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal));

    let keep = match interpolation {
        HorizontalInterpolation::Nearest => 1,
        HorizontalInterpolation::Bilinear => INTERPOLATED_NEIGHBOR_COUNT.min(nearest.len()),
    };
    let nearest = &nearest[..keep.max(1)];

    if nearest[0].1 <= 1.0e-12 || matches!(interpolation, HorizontalInterpolation::Nearest) {
        return SampleStencil {
            indices: vec![nearest[0].0],
            weights: vec![1.0],
        };
    }

    let mut weights = nearest
        .iter()
        .map(|(_, distance)| 1.0 / distance.max(1.0e-12))
        .collect::<Vec<_>>();
    let weight_sum = weights.iter().sum::<f64>().max(1.0e-12);
    for weight in &mut weights {
        *weight /= weight_sum;
    }

    SampleStencil {
        indices: nearest.iter().map(|(idx, _)| *idx).collect(),
        weights,
    }
}

fn geographic_distance_score(surface: &SurfaceFields, idx: usize, point: GeoPoint) -> f64 {
    let cos_lat = point.lat_deg.to_radians().cos().abs().max(0.2);
    let dlat = surface.lat[idx] - point.lat_deg;
    let dlon = normalized_longitude_delta(surface.lon[idx] - point.lon_deg) * cos_lat;
    dlat * dlat + dlon * dlon
}

fn sample_weighted_2d(values: &[f64], stencil: &SampleStencil) -> f64 {
    sample_weighted_indices(values, 0, stencil)
}

fn sample_weighted_level(values: &[f64], level_offset: usize, stencil: &SampleStencil) -> f64 {
    sample_weighted_indices(values, level_offset, stencil)
}

fn sample_weighted_indices(values: &[f64], base_offset: usize, stencil: &SampleStencil) -> f64 {
    let mut weighted_sum = 0.0;
    let mut weight_sum = 0.0;
    for (&idx, &weight) in stencil.indices.iter().zip(stencil.weights.iter()) {
        let value = values[base_offset + idx];
        if value.is_finite() {
            weighted_sum += value * weight;
            weight_sum += weight;
        }
    }
    if weight_sum <= 0.0 {
        f64::NAN
    } else {
        weighted_sum / weight_sum
    }
}

fn candidate_indices(
    surface: &SurfaceFields,
    sampled_points: &[GeoPoint],
    padding_deg: f64,
) -> Vec<usize> {
    let (west, east, south, north) = bounds_for_points(sampled_points, padding_deg);
    let mut indices = Vec::new();
    for (idx, (&lat, &lon)) in surface.lat.iter().zip(surface.lon.iter()).enumerate() {
        if lat >= south && lat <= north && lon >= west && lon <= east {
            indices.push(idx);
        }
    }
    if indices.is_empty() {
        (0..surface.lat.len()).collect()
    } else {
        indices
    }
}

fn bounds_for_points(points: &[GeoPoint], padding_deg: f64) -> (f64, f64, f64, f64) {
    let mut west = f64::INFINITY;
    let mut east = f64::NEG_INFINITY;
    let mut south = f64::INFINITY;
    let mut north = f64::NEG_INFINITY;
    for point in points {
        west = west.min(point.lon_deg);
        east = east.max(point.lon_deg);
        south = south.min(point.lat_deg);
        north = north.max(point.lat_deg);
    }
    (
        west - padding_deg,
        east + padding_deg,
        south - padding_deg,
        north + padding_deg,
    )
}

fn normalized_longitude_delta(delta_deg: f64) -> f64 {
    let mut delta = delta_deg;
    while delta <= -180.0 {
        delta += 360.0;
    }
    while delta > 180.0 {
        delta -= 360.0;
    }
    delta
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustwx_cross_section::{CrossSectionRequest, SamplingStrategy, SectionPath};

    #[test]
    fn supported_product_list_matches_current_pressure_section_lane() {
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::Temperature
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::RelativeHumidity
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::ThetaE
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::WindSpeed
        ));
        assert!(!supports_pressure_cross_section_product(
            CrossSectionProduct::Omega
        ));
    }

    #[test]
    fn pressure_cross_section_builder_returns_finite_theta_e_and_wind_overlay() {
        let surface = SurfaceFields {
            lat: vec![35.0, 35.0, 36.0, 36.0],
            lon: vec![-100.0, -99.0, -100.0, -99.0],
            nx: 2,
            ny: 2,
            projection: None,
            psfc_pa: vec![100000.0, 99500.0, 99000.0, 98500.0],
            orog_m: vec![400.0, 450.0, 600.0, 650.0],
            orog_is_proxy: false,
            t2_k: vec![298.0; 4],
            q2_kgkg: vec![0.012; 4],
            u10_ms: vec![8.0; 4],
            v10_ms: vec![4.0; 4],
        };
        let pressure = PressureFields {
            pressure_levels_hpa: vec![1000.0, 850.0],
            temperature_c_3d: vec![24.0, 26.0, 22.0, 24.0, 12.0, 14.0, 10.0, 12.0],
            qvapor_kgkg_3d: vec![0.014, 0.013, 0.012, 0.011, 0.010, 0.009, 0.008, 0.007],
            u_ms_3d: vec![12.0, 16.0, 14.0, 18.0, 20.0, 24.0, 22.0, 26.0],
            v_ms_3d: vec![2.0, 4.0, 3.0, 5.0, 6.0, 8.0, 7.0, 9.0],
            gh_m_3d: vec![100.0; 8],
        };
        let layout = CrossSectionRequest::new(
            SectionPath::endpoints(
                GeoPoint::new(35.0, -100.0).unwrap(),
                GeoPoint::new(36.0, -99.0).unwrap(),
            )
            .unwrap(),
        )
        .with_sampling(SamplingStrategy::Count(3))
        .with_metadata(
            SectionMetadata::new()
                .with_attribute("route_label", "TEST ROUTE")
                .with_attribute("start_label", "35.00N 100.00W")
                .with_attribute("end_label", "36.00N 99.00W"),
        )
        .build_layout()
        .unwrap();

        let artifact = build_pressure_cross_section_from_parts(
            &surface,
            &pressure,
            ModelId::Hrrr,
            SourceId::Nomads,
            &CycleSpec::new("20260414", 23).unwrap(),
            0,
            &layout,
            CrossSectionProduct::ThetaE,
        )
        .unwrap();

        assert_eq!(artifact.style.product(), CrossSectionProduct::ThetaE);
        assert_eq!(artifact.section.n_points(), 3);
        assert_eq!(artifact.section.n_levels(), 2);
        assert!(
            artifact
                .section
                .values()
                .iter()
                .all(|value| value.is_finite())
        );
        assert_eq!(
            artifact.section.metadata().attribute("product_key"),
            Some("theta_e")
        );
        assert_eq!(artifact.wind_overlay.grid.n_levels(), 2);
        assert_eq!(artifact.wind_overlay.grid.n_points(), 3);
    }

    #[test]
    fn wind_speed_sections_are_converted_to_knots() {
        let values = build_product_values(
            CrossSectionProduct::WindSpeed,
            &[1000.0],
            &[20.0],
            &[0.010],
            &[10.0],
            &[0.0],
        )
        .unwrap();

        assert_eq!(values.len(), 1);
        assert!((values[0] - 19.438_444_924_406_05).abs() < 1.0e-6);
    }
}
