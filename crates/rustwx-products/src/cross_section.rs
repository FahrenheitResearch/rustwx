use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::time::Instant;

use rustwx_calc::{
    compute_dewpoint_from_pressure_and_mixing_ratio,
    compute_relative_humidity_from_pressure_temperature_and_mixing_ratio,
    compute_theta_e_from_pressure_temperature_and_mixing_ratio,
};
use rustwx_core::{CycleSpec, ModelId, SourceId};
use rustwx_cross_section::{
    CrossSectionProduct, CrossSectionStyle, DecomposedWindGrid, GeoPoint, HorizontalInterpolation,
    ScalarSection, SectionLayout, SectionMetadata, TerrainProfile, VerticalAxis, VerticalKind,
    VerticalScale, VerticalUnits, WindOverlayBundle, WindOverlayStyle, decompose_wind_grid,
};
use serde::{Deserialize, Serialize};

use crate::gridded::{LoadedModelTimestep, PressureFields, SurfaceFields};

const SECTION_CANDIDATE_PADDING_DEG: f64 = 1.5;
const INTERPOLATED_NEIGHBOR_COUNT: usize = 4;
const MS_TO_KT: f64 = 1.943_844_492_440_604_8;
const PA_S_TO_HPA_HR: f64 = 36.0;

pub const SUPPORTED_PRESSURE_CROSS_SECTION_PRODUCTS: [CrossSectionProduct; 10] = [
    CrossSectionProduct::Temperature,
    CrossSectionProduct::RelativeHumidity,
    CrossSectionProduct::SpecificHumidity,
    CrossSectionProduct::ThetaE,
    CrossSectionProduct::WindSpeed,
    CrossSectionProduct::WetBulb,
    CrossSectionProduct::VaporPressureDeficit,
    CrossSectionProduct::DewpointDepression,
    CrossSectionProduct::MoistureTransport,
    CrossSectionProduct::FireWeather,
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionFacts {
    pub route: PressureCrossSectionRouteFacts,
    pub metadata: PressureCrossSectionMetadataFacts,
    pub scalar: PressureCrossSectionScalarFacts,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terrain: Option<PressureCrossSectionTerrainFacts>,
    pub wind: PressureCrossSectionWindFacts,
}

impl PressureCrossSectionFacts {
    pub fn from_artifact(layout: &SectionLayout, artifact: &PressureCrossSectionArtifact) -> Self {
        summarize_pressure_cross_section_artifact(layout, artifact)
    }

    pub fn global_minimum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.scalar.global_minimum()
    }

    pub fn global_maximum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.scalar.global_maximum()
    }

    pub fn lowest_visible_level_minimum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.scalar.lowest_visible_level_minimum()
    }

    pub fn lowest_visible_level_maximum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.scalar.lowest_visible_level_maximum()
    }

    pub fn strongest_wind_speed(&self) -> Option<&PressureCrossSectionValueFact> {
        self.wind.strongest_speed()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionRouteFacts {
    pub total_distance_km: f64,
    pub sample_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean_sample_spacing_km: Option<f64>,
    pub start: PressureCrossSectionRouteSample,
    pub midpoint: PressureCrossSectionRouteSample,
    pub end: PressureCrossSectionRouteSample,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionRouteSample {
    pub sample_index: usize,
    pub distance_km: f64,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub bearing_deg: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionMetadataFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_units: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_label: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionScalarFacts {
    pub vertical_kind: String,
    pub vertical_units: String,
    pub vertical_scale: String,
    pub level_count: usize,
    pub top_level: f64,
    pub bottom_level: f64,
    pub finite_value_count: usize,
    pub missing_value_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_minimum: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_maximum: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowest_visible_level_minimum: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lowest_visible_level_maximum: Option<PressureCrossSectionValueFact>,
}

impl PressureCrossSectionScalarFacts {
    pub fn global_minimum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.global_minimum.as_ref()
    }

    pub fn global_maximum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.global_maximum.as_ref()
    }

    pub fn lowest_visible_level_minimum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.lowest_visible_level_minimum.as_ref()
    }

    pub fn lowest_visible_level_maximum(&self) -> Option<&PressureCrossSectionValueFact> {
        self.lowest_visible_level_maximum.as_ref()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionValueFact {
    pub value: f64,
    pub level_index: usize,
    pub vertical_value: f64,
    pub sample_index: usize,
    pub distance_km: f64,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub bearing_deg: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionTerrainFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_pressure_minimum: Option<PressureCrossSectionProfileValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_pressure_maximum: Option<PressureCrossSectionProfileValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_height_minimum_m: Option<PressureCrossSectionProfileValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_height_maximum_m: Option<PressureCrossSectionProfileValueFact>,
}

impl PressureCrossSectionTerrainFacts {
    pub fn surface_pressure_minimum(&self) -> Option<&PressureCrossSectionProfileValueFact> {
        self.surface_pressure_minimum.as_ref()
    }

    pub fn surface_pressure_maximum(&self) -> Option<&PressureCrossSectionProfileValueFact> {
        self.surface_pressure_maximum.as_ref()
    }

    pub fn surface_height_minimum_m(&self) -> Option<&PressureCrossSectionProfileValueFact> {
        self.surface_height_minimum_m.as_ref()
    }

    pub fn surface_height_maximum_m(&self) -> Option<&PressureCrossSectionProfileValueFact> {
        self.surface_height_maximum_m.as_ref()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionProfileValueFact {
    pub value: f64,
    pub sample_index: usize,
    pub distance_km: f64,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub bearing_deg: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureCrossSectionWindFacts {
    pub units: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_speed: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_tailwind: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_headwind: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_left_crosswind: Option<PressureCrossSectionValueFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_right_crosswind: Option<PressureCrossSectionValueFact>,
}

impl PressureCrossSectionWindFacts {
    pub fn strongest_speed(&self) -> Option<&PressureCrossSectionValueFact> {
        self.strongest_speed.as_ref()
    }

    pub fn strongest_tailwind(&self) -> Option<&PressureCrossSectionValueFact> {
        self.strongest_tailwind.as_ref()
    }

    pub fn strongest_headwind(&self) -> Option<&PressureCrossSectionValueFact> {
        self.strongest_headwind.as_ref()
    }

    pub fn strongest_left_crosswind(&self) -> Option<&PressureCrossSectionValueFact> {
        self.strongest_left_crosswind.as_ref()
    }

    pub fn strongest_right_crosswind(&self) -> Option<&PressureCrossSectionValueFact> {
        self.strongest_right_crosswind.as_ref()
    }
}

/// Optional fields that future native/hybrid cross-section builders can forward
/// into the shared product-derivation lane without changing the product API.
#[derive(Debug, Clone, Copy, Default)]
pub struct PressureCrossSectionOptionalProductFields<'a> {
    pub omega_pa_s: Option<&'a [f64]>,
    pub smoke_ugm3: Option<&'a [f64]>,
}

/// Shared sampled-path inputs for pressure/native cross-section products.
#[derive(Debug, Clone, Copy)]
pub struct PressureCrossSectionProductInputs<'a> {
    pub pressure_hpa: &'a [f64],
    pub temperature_c: &'a [f64],
    pub mixing_ratio_kgkg: &'a [f64],
    pub u_ms: &'a [f64],
    pub v_ms: &'a [f64],
    pub optional: PressureCrossSectionOptionalProductFields<'a>,
}

impl<'a> PressureCrossSectionProductInputs<'a> {
    pub fn len(&self) -> usize {
        self.pressure_hpa.len()
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let expected = self.len();
        validate_product_input_length("temperature_c", self.temperature_c.len(), expected)?;
        validate_product_input_length("mixing_ratio_kgkg", self.mixing_ratio_kgkg.len(), expected)?;
        validate_product_input_length("u_ms", self.u_ms.len(), expected)?;
        validate_product_input_length("v_ms", self.v_ms.len(), expected)?;
        if let Some(omega_pa_s) = self.optional.omega_pa_s {
            validate_product_input_length("omega_pa_s", omega_pa_s.len(), expected)?;
        }
        if let Some(smoke_ugm3) = self.optional.smoke_ugm3 {
            validate_product_input_length("smoke_ugm3", smoke_ugm3.len(), expected)?;
        }
        Ok(())
    }
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

pub fn build_pressure_cross_section_facts(
    loaded: &LoadedModelTimestep,
    layout: &SectionLayout,
    product: CrossSectionProduct,
) -> Result<PressureCrossSectionFacts, Box<dyn std::error::Error>> {
    let artifact = build_pressure_cross_section(loaded, layout, product)?;
    Ok(summarize_pressure_cross_section_artifact(layout, &artifact))
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
    let mut mixing_ratio_kgkg = Vec::with_capacity(n_levels * n_points);
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
            mixing_ratio_kgkg.push(sample_weighted_level(
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
    let section_values = build_pressure_cross_section_product_values(
        product,
        PressureCrossSectionProductInputs {
            pressure_hpa: &pressure_hpa,
            temperature_c: &temperature_c,
            mixing_ratio_kgkg: &mixing_ratio_kgkg,
            u_ms: &u_ms,
            v_ms: &v_ms,
            optional: PressureCrossSectionOptionalProductFields::default(),
        },
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

pub fn summarize_pressure_cross_section_artifact(
    layout: &SectionLayout,
    artifact: &PressureCrossSectionArtifact,
) -> PressureCrossSectionFacts {
    let masked_section = artifact.section.masked_with_terrain();
    PressureCrossSectionFacts {
        route: summarize_route_facts(layout),
        metadata: summarize_metadata_facts(masked_section.metadata()),
        scalar: summarize_scalar_facts(layout, &masked_section),
        terrain: summarize_terrain_facts(layout, masked_section.terrain()),
        wind: summarize_wind_facts(layout, &masked_section, &artifact.wind_overlay.grid),
    }
}

pub fn build_pressure_cross_section_product_values(
    product: CrossSectionProduct,
    inputs: PressureCrossSectionProductInputs<'_>,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    inputs.validate()?;
    match product {
        CrossSectionProduct::Temperature => Ok(inputs.temperature_c.to_vec()),
        CrossSectionProduct::RelativeHumidity => Ok(
            compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.temperature_c,
                inputs.mixing_ratio_kgkg,
            )?,
        ),
        CrossSectionProduct::SpecificHumidity => {
            Ok(mixing_ratio_to_specific_humidity_gkg(inputs.mixing_ratio_kgkg))
        }
        CrossSectionProduct::ThetaE => {
            Ok(compute_theta_e_from_pressure_temperature_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.temperature_c,
                inputs.mixing_ratio_kgkg,
            )?)
        }
        CrossSectionProduct::WindSpeed => Ok(compute_wind_speed_kt(inputs.u_ms, inputs.v_ms)?),
        CrossSectionProduct::WetBulb => {
            let relative_humidity_pct = compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.temperature_c,
                inputs.mixing_ratio_kgkg,
            )?;
            compute_wet_bulb_temperature_c(inputs.temperature_c, &relative_humidity_pct)
        }
        CrossSectionProduct::VaporPressureDeficit => {
            let relative_humidity_pct = compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.temperature_c,
                inputs.mixing_ratio_kgkg,
            )?;
            compute_vapor_pressure_deficit_hpa(inputs.temperature_c, &relative_humidity_pct)
        }
        CrossSectionProduct::DewpointDepression => {
            let dewpoint_c = compute_dewpoint_from_pressure_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.mixing_ratio_kgkg,
            )?;
            Ok(inputs
                .temperature_c
                .iter()
                .zip(dewpoint_c.iter())
                .map(|(&temperature_c, &dewpoint_c)| temperature_c - dewpoint_c)
                .collect())
        }
        CrossSectionProduct::MoistureTransport => {
            let specific_humidity_gkg = mixing_ratio_to_specific_humidity_gkg(inputs.mixing_ratio_kgkg);
            let wind_speed_ms = compute_wind_speed_ms(inputs.u_ms, inputs.v_ms)?;
            Ok(specific_humidity_gkg
                .iter()
                .zip(wind_speed_ms.iter())
                .map(|(&specific_humidity_gkg, &wind_speed_ms)| specific_humidity_gkg * wind_speed_ms)
                .collect())
        }
        CrossSectionProduct::FireWeather => Ok(
            compute_relative_humidity_from_pressure_temperature_and_mixing_ratio(
                inputs.pressure_hpa,
                inputs.temperature_c,
                inputs.mixing_ratio_kgkg,
            )?,
        ),
        CrossSectionProduct::Omega => inputs
            .optional
            .omega_pa_s
            .map(|omega_pa_s| omega_pa_s.iter().map(|&value| value * PA_S_TO_HPA_HR).collect())
            .ok_or_else(|| {
                "cross-section product 'omega' requires sampled omega input from an upstream/native helper"
                    .into()
            }),
        CrossSectionProduct::Smoke => inputs
            .optional
            .smoke_ugm3
            .map(|smoke_ugm3| smoke_ugm3.to_vec())
            .ok_or_else(|| {
                "cross-section product 'smoke' requires sampled smoke input from an upstream/native helper"
                    .into()
            }),
        _ => Err(format!(
            "cross-section product '{}' is not supported by the gridded pressure builder",
            product.slug()
        )
        .into()),
    }
}

fn validate_product_input_length(
    field: &str,
    actual: usize,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if actual != expected {
        Err(format!(
            "pressure cross-section product input '{field}' had length {actual}, expected {expected}"
        )
        .into())
    } else {
        Ok(())
    }
}

fn mixing_ratio_to_specific_humidity_gkg(mixing_ratio_kgkg: &[f64]) -> Vec<f64> {
    mixing_ratio_kgkg
        .iter()
        .map(|&mixing_ratio_kgkg| {
            let specific_humidity_kgkg = mixing_ratio_kgkg / (1.0 + mixing_ratio_kgkg);
            specific_humidity_kgkg * 1000.0
        })
        .collect()
}

fn compute_wind_speed_ms(
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    validate_product_input_length("v_ms", v_ms.len(), u_ms.len())?;
    Ok(u_ms
        .iter()
        .zip(v_ms.iter())
        .map(|(&u_ms, &v_ms)| (u_ms * u_ms + v_ms * v_ms).sqrt())
        .collect())
}

fn compute_wind_speed_kt(
    u_ms: &[f64],
    v_ms: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    Ok(compute_wind_speed_ms(u_ms, v_ms)?
        .into_iter()
        .map(|wind_speed_ms| wind_speed_ms * MS_TO_KT)
        .collect())
}

fn compute_wet_bulb_temperature_c(
    temperature_c: &[f64],
    relative_humidity_pct: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    validate_product_input_length(
        "relative_humidity_pct",
        relative_humidity_pct.len(),
        temperature_c.len(),
    )?;
    Ok(temperature_c
        .iter()
        .zip(relative_humidity_pct.iter())
        .map(|(&temperature_c, &relative_humidity_pct)| {
            approximate_wet_bulb_temperature_c(temperature_c, relative_humidity_pct)
        })
        .collect())
}

fn approximate_wet_bulb_temperature_c(temperature_c: f64, relative_humidity_pct: f64) -> f64 {
    let relative_humidity_pct = relative_humidity_pct.clamp(0.0, 100.0);
    temperature_c * (0.151_977 * (relative_humidity_pct + 8.313_659).sqrt()).atan()
        + (temperature_c + relative_humidity_pct).atan()
        - (relative_humidity_pct - 1.676_331).atan()
        + 0.003_918_38
            * relative_humidity_pct.powf(1.5)
            * (0.023_101 * relative_humidity_pct).atan()
        - 4.686_035
}

fn compute_vapor_pressure_deficit_hpa(
    temperature_c: &[f64],
    relative_humidity_pct: &[f64],
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    validate_product_input_length(
        "relative_humidity_pct",
        relative_humidity_pct.len(),
        temperature_c.len(),
    )?;
    Ok(temperature_c
        .iter()
        .zip(relative_humidity_pct.iter())
        .map(|(&temperature_c, &relative_humidity_pct)| {
            let saturation_vapor_pressure_hpa = tetens_saturation_vapor_pressure_hpa(temperature_c);
            let relative_humidity_fraction = (relative_humidity_pct / 100.0).clamp(0.0, 1.0);
            (saturation_vapor_pressure_hpa * (1.0 - relative_humidity_fraction)).max(0.0)
        })
        .collect())
}

fn tetens_saturation_vapor_pressure_hpa(temperature_c: f64) -> f64 {
    6.1078 * (17.27 * temperature_c / (temperature_c + 237.3)).exp()
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

fn summarize_route_facts(layout: &SectionLayout) -> PressureCrossSectionRouteFacts {
    let sample_count = layout.sampled_path.samples.len();
    let midpoint_index = sample_count.saturating_sub(1) / 2;
    PressureCrossSectionRouteFacts {
        total_distance_km: layout.sampled_path.total_distance_km,
        sample_count,
        mean_sample_spacing_km: (sample_count >= 2)
            .then_some(layout.sampled_path.total_distance_km / (sample_count as f64 - 1.0)),
        start: route_sample_fact(layout, 0),
        midpoint: route_sample_fact(layout, midpoint_index),
        end: route_sample_fact(layout, sample_count.saturating_sub(1)),
    }
}

fn summarize_metadata_facts(metadata: &SectionMetadata) -> PressureCrossSectionMetadataFacts {
    PressureCrossSectionMetadataFacts {
        title: metadata.title.clone(),
        field_name: metadata.field_name.clone(),
        field_units: metadata.field_units.clone(),
        source: metadata.source.clone(),
        valid_label: metadata.valid_label.clone(),
        attributes: metadata.attributes.clone(),
    }
}

fn summarize_scalar_facts(
    layout: &SectionLayout,
    section: &ScalarSection,
) -> PressureCrossSectionScalarFacts {
    let axis = section.vertical_axis();
    let mut finite_value_count = 0usize;
    let mut global_minimum = None::<PressureCrossSectionValueFact>;
    let mut global_maximum = None::<PressureCrossSectionValueFact>;

    for level_index in 0..section.n_levels() {
        for point_index in 0..section.n_points() {
            let Some(value) = section.value(level_index, point_index) else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            finite_value_count = finite_value_count.saturating_add(1);
            let fact = section_value_fact(layout, axis, point_index, level_index, value as f64);
            update_value_minimum(&mut global_minimum, fact.clone());
            update_value_maximum(&mut global_maximum, fact);
        }
    }

    let mut lowest_visible_level_minimum = None::<PressureCrossSectionValueFact>;
    let mut lowest_visible_level_maximum = None::<PressureCrossSectionValueFact>;
    for point_index in 0..section.n_points() {
        let mut best_level_index = None::<usize>;
        let mut best_plot_fraction = f64::NEG_INFINITY;
        for (level_index, &vertical_value) in axis.levels().iter().enumerate() {
            let Some(value) = section.value(level_index, point_index) else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            let Some(plot_fraction) = axis.plot_fraction_of_value(vertical_value) else {
                continue;
            };
            if plot_fraction > best_plot_fraction {
                best_plot_fraction = plot_fraction;
                best_level_index = Some(level_index);
            }
        }

        let Some(level_index) = best_level_index else {
            continue;
        };
        let value = section
            .value(level_index, point_index)
            .expect("section dimensions should stay internally consistent")
            as f64;
        let fact = section_value_fact(layout, axis, point_index, level_index, value);
        update_value_minimum(&mut lowest_visible_level_minimum, fact.clone());
        update_value_maximum(&mut lowest_visible_level_maximum, fact);
    }

    PressureCrossSectionScalarFacts {
        vertical_kind: vertical_kind_slug(axis.kind()).to_string(),
        vertical_units: vertical_units_slug(axis.units()).to_string(),
        vertical_scale: vertical_scale_slug(axis.scale()).to_string(),
        level_count: axis.len(),
        top_level: axis.plot_top(),
        bottom_level: axis.plot_bottom(),
        finite_value_count,
        missing_value_count: section.values().len().saturating_sub(finite_value_count),
        global_minimum,
        global_maximum,
        lowest_visible_level_minimum,
        lowest_visible_level_maximum,
    }
}

fn summarize_terrain_facts(
    layout: &SectionLayout,
    terrain: Option<&TerrainProfile>,
) -> Option<PressureCrossSectionTerrainFacts> {
    let terrain = terrain?;
    Some(PressureCrossSectionTerrainFacts {
        surface_pressure_minimum: profile_minimum_fact(layout, terrain.surface_pressure_hpa()),
        surface_pressure_maximum: profile_maximum_fact(layout, terrain.surface_pressure_hpa()),
        surface_height_minimum_m: profile_minimum_fact(layout, terrain.surface_height_m()),
        surface_height_maximum_m: profile_maximum_fact(layout, terrain.surface_height_m()),
    })
}

fn summarize_wind_facts(
    layout: &SectionLayout,
    section: &ScalarSection,
    wind: &DecomposedWindGrid,
) -> PressureCrossSectionWindFacts {
    let mut facts = PressureCrossSectionWindFacts {
        units: "m/s".to_string(),
        ..PressureCrossSectionWindFacts::default()
    };
    let axis = section.vertical_axis();

    for level_index in 0..wind.n_levels() {
        for point_index in 0..wind.n_points() {
            if !section
                .value(level_index, point_index)
                .map(|value| value.is_finite())
                .unwrap_or(false)
            {
                continue;
            }

            if let Some(value) = wind.speed_value(level_index, point_index) {
                if value.is_finite() {
                    update_value_maximum(
                        &mut facts.strongest_speed,
                        section_value_fact(layout, axis, point_index, level_index, value as f64),
                    );
                }
            }

            if let Some(value) = wind.along_section_value(level_index, point_index) {
                if value.is_finite() {
                    let fact =
                        section_value_fact(layout, axis, point_index, level_index, value as f64);
                    if value > 0.0 {
                        update_value_maximum(&mut facts.strongest_tailwind, fact);
                    } else if value < 0.0 {
                        update_value_minimum(&mut facts.strongest_headwind, fact);
                    }
                }
            }

            if let Some(value) = wind.left_of_section_value(level_index, point_index) {
                if value.is_finite() {
                    let fact =
                        section_value_fact(layout, axis, point_index, level_index, value as f64);
                    if value > 0.0 {
                        update_value_maximum(&mut facts.strongest_left_crosswind, fact);
                    } else if value < 0.0 {
                        update_value_minimum(&mut facts.strongest_right_crosswind, fact);
                    }
                }
            }
        }
    }

    facts
}

fn route_sample_fact(
    layout: &SectionLayout,
    point_index: usize,
) -> PressureCrossSectionRouteSample {
    let sample = layout.sampled_path.samples[point_index];
    PressureCrossSectionRouteSample {
        sample_index: point_index,
        distance_km: sample.distance_km,
        latitude_deg: sample.point.lat_deg,
        longitude_deg: sample.point.lon_deg,
        bearing_deg: sample.bearing_deg,
    }
}

fn section_value_fact(
    layout: &SectionLayout,
    axis: &VerticalAxis,
    point_index: usize,
    level_index: usize,
    value: f64,
) -> PressureCrossSectionValueFact {
    let route_sample = route_sample_fact(layout, point_index);
    PressureCrossSectionValueFact {
        value,
        level_index,
        vertical_value: axis.levels()[level_index],
        sample_index: route_sample.sample_index,
        distance_km: route_sample.distance_km,
        latitude_deg: route_sample.latitude_deg,
        longitude_deg: route_sample.longitude_deg,
        bearing_deg: route_sample.bearing_deg,
    }
}

fn profile_value_fact(
    layout: &SectionLayout,
    point_index: usize,
    value: f64,
) -> PressureCrossSectionProfileValueFact {
    let route_sample = route_sample_fact(layout, point_index);
    PressureCrossSectionProfileValueFact {
        value,
        sample_index: route_sample.sample_index,
        distance_km: route_sample.distance_km,
        latitude_deg: route_sample.latitude_deg,
        longitude_deg: route_sample.longitude_deg,
        bearing_deg: route_sample.bearing_deg,
    }
}

fn profile_minimum_fact(
    layout: &SectionLayout,
    values: Option<&[f64]>,
) -> Option<PressureCrossSectionProfileValueFact> {
    let values = values?;
    let mut minimum = None::<PressureCrossSectionProfileValueFact>;
    for (point_index, &value) in values.iter().enumerate() {
        if !value.is_finite() {
            continue;
        }
        update_profile_minimum(&mut minimum, profile_value_fact(layout, point_index, value));
    }
    minimum
}

fn profile_maximum_fact(
    layout: &SectionLayout,
    values: Option<&[f64]>,
) -> Option<PressureCrossSectionProfileValueFact> {
    let values = values?;
    let mut maximum = None::<PressureCrossSectionProfileValueFact>;
    for (point_index, &value) in values.iter().enumerate() {
        if !value.is_finite() {
            continue;
        }
        update_profile_maximum(&mut maximum, profile_value_fact(layout, point_index, value));
    }
    maximum
}

fn update_value_minimum(
    target: &mut Option<PressureCrossSectionValueFact>,
    candidate: PressureCrossSectionValueFact,
) {
    if target
        .as_ref()
        .map(|current| {
            candidate.value < current.value
                || equivalent_value_fact(candidate.value, current.value)
                    && candidate.sample_index < current.sample_index
        })
        .unwrap_or(true)
    {
        *target = Some(candidate);
    }
}

fn update_value_maximum(
    target: &mut Option<PressureCrossSectionValueFact>,
    candidate: PressureCrossSectionValueFact,
) {
    if target
        .as_ref()
        .map(|current| {
            candidate.value > current.value
                || equivalent_value_fact(candidate.value, current.value)
                    && candidate.sample_index < current.sample_index
        })
        .unwrap_or(true)
    {
        *target = Some(candidate);
    }
}

fn update_profile_minimum(
    target: &mut Option<PressureCrossSectionProfileValueFact>,
    candidate: PressureCrossSectionProfileValueFact,
) {
    if target
        .as_ref()
        .map(|current| {
            candidate.value < current.value
                || equivalent_value_fact(candidate.value, current.value)
                    && candidate.sample_index < current.sample_index
        })
        .unwrap_or(true)
    {
        *target = Some(candidate);
    }
}

fn update_profile_maximum(
    target: &mut Option<PressureCrossSectionProfileValueFact>,
    candidate: PressureCrossSectionProfileValueFact,
) {
    if target
        .as_ref()
        .map(|current| {
            candidate.value > current.value
                || equivalent_value_fact(candidate.value, current.value)
                    && candidate.sample_index < current.sample_index
        })
        .unwrap_or(true)
    {
        *target = Some(candidate);
    }
}

fn equivalent_value_fact(left: f64, right: f64) -> bool {
    (left - right).abs() <= f64::EPSILON
}

fn vertical_kind_slug(kind: VerticalKind) -> &'static str {
    match kind {
        VerticalKind::Pressure => "pressure",
        VerticalKind::Height => "height",
    }
}

fn vertical_units_slug(units: VerticalUnits) -> &'static str {
    match units {
        VerticalUnits::Hectopascals => "hpa",
        VerticalUnits::Meters => "meters",
        VerticalUnits::Kilometers => "kilometers",
    }
}

fn vertical_scale_slug(scale: VerticalScale) -> &'static str {
    match scale {
        VerticalScale::Linear => "linear",
        VerticalScale::Logarithmic => "logarithmic",
    }
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

#[derive(Debug, Clone, Copy)]
struct SampleStencil {
    len: u8,
    indices: [usize; INTERPOLATED_NEIGHBOR_COUNT],
    weights: [f64; INTERPOLATED_NEIGHBOR_COUNT],
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
    let keep = match interpolation {
        HorizontalInterpolation::Nearest => 1,
        HorizontalInterpolation::Bilinear => INTERPOLATED_NEIGHBOR_COUNT,
    };
    let keep = keep.max(1);
    let mut nearest = [(usize::MAX, f64::INFINITY); INTERPOLATED_NEIGHBOR_COUNT];
    let mut nearest_len = 0usize;
    for &idx in candidates {
        let distance = geographic_distance_score(surface, idx, point);
        insert_best_candidate(&mut nearest, &mut nearest_len, keep, idx, distance);
    }
    let nearest_len = nearest_len.max(1);

    if nearest[0].1 <= 1.0e-12 || matches!(interpolation, HorizontalInterpolation::Nearest) {
        let mut indices = [0usize; INTERPOLATED_NEIGHBOR_COUNT];
        indices[0] = nearest[0].0;
        return SampleStencil {
            len: 1,
            indices,
            weights: [1.0, 0.0, 0.0, 0.0],
        };
    }

    let mut indices = [0usize; INTERPOLATED_NEIGHBOR_COUNT];
    let mut weights = [0.0; INTERPOLATED_NEIGHBOR_COUNT];
    let mut weight_sum = 0.0;
    for slot in 0..nearest_len {
        indices[slot] = nearest[slot].0;
        weights[slot] = 1.0 / nearest[slot].1.max(1.0e-12);
        weight_sum += weights[slot];
    }
    let weight_sum = weight_sum.max(1.0e-12);
    for weight in &mut weights[..nearest_len] {
        *weight /= weight_sum;
    }

    SampleStencil {
        len: nearest_len as u8,
        indices,
        weights,
    }
}

fn insert_best_candidate(
    nearest: &mut [(usize, f64); INTERPOLATED_NEIGHBOR_COUNT],
    nearest_len: &mut usize,
    keep: usize,
    idx: usize,
    distance: f64,
) {
    let keep = keep.min(INTERPOLATED_NEIGHBOR_COUNT).max(1);
    let mut insert_at = *nearest_len;
    for slot in 0..*nearest_len {
        match distance
            .partial_cmp(&nearest[slot].1)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Less => {
                insert_at = slot;
                break;
            }
            Ordering::Equal if idx < nearest[slot].0 => {
                insert_at = slot;
                break;
            }
            Ordering::Equal | Ordering::Greater => {}
        }
    }
    if insert_at >= keep {
        return;
    }

    let old_len = *nearest_len;
    let new_len = (old_len + 1).min(keep);
    for slot in (insert_at..new_len.saturating_sub(1)).rev() {
        nearest[slot + 1] = nearest[slot];
    }
    nearest[insert_at] = (idx, distance);
    *nearest_len = new_len;
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
    for slot in 0..stencil.len as usize {
        let idx = stencil.indices[slot];
        let weight = stencil.weights[slot];
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
    use rustwx_cross_section::{
        CrossSectionRequest, CrossSectionStyle, SamplingStrategy, ScalarSection, SectionPath,
        TerrainProfile, VerticalAxis, WindOverlayBundle, WindOverlayStyle, decompose_wind_grid,
    };

    fn sample_surface_fields() -> SurfaceFields {
        SurfaceFields {
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
        }
    }

    fn sample_pressure_fields() -> PressureFields {
        PressureFields {
            pressure_levels_hpa: vec![1000.0, 850.0],
            temperature_c_3d: vec![24.0, 26.0, 22.0, 24.0, 12.0, 14.0, 10.0, 12.0],
            qvapor_kgkg_3d: vec![0.014, 0.013, 0.012, 0.011, 0.010, 0.009, 0.008, 0.007],
            u_ms_3d: vec![12.0, 16.0, 14.0, 18.0, 20.0, 24.0, 22.0, 26.0],
            v_ms_3d: vec![2.0, 4.0, 3.0, 5.0, 6.0, 8.0, 7.0, 9.0],
            gh_m_3d: vec![100.0; 8],
        }
    }

    fn sample_layout() -> SectionLayout {
        CrossSectionRequest::new(
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
        .unwrap()
    }

    fn below_ground_extrema_fixture() -> (SectionLayout, PressureCrossSectionArtifact) {
        let layout = CrossSectionRequest::new(
            SectionPath::endpoints(
                GeoPoint::new(35.0, -100.0).unwrap(),
                GeoPoint::new(36.0, -99.0).unwrap(),
            )
            .unwrap(),
        )
        .with_sampling(SamplingStrategy::Count(2))
        .build_layout()
        .unwrap();
        let terrain = TerrainProfile::new(layout.sampled_path.distances_km())
            .unwrap()
            .with_surface_pressure_hpa(vec![950.0, 950.0])
            .unwrap()
            .with_surface_height_m(vec![150.0, 250.0])
            .unwrap();
        let section = ScalarSection::new(
            layout.sampled_path.distances_km(),
            VerticalAxis::pressure_hpa(vec![1000.0, 900.0]).unwrap(),
            vec![999.0, 999.0, 10.0, 20.0],
        )
        .unwrap()
        .with_metadata(
            SectionMetadata::new()
                .field("temperature", "C")
                .with_attribute("route_label", "MASK TEST"),
        )
        .with_terrain(terrain)
        .unwrap();
        let wind_overlay = WindOverlayBundle::new(
            decompose_wind_grid(
                &[0.0, 0.0, 5.0, 7.0],
                &[0.0, 0.0, 0.0, 0.0],
                2,
                2,
                &[45.0, 45.0],
            )
            .unwrap(),
            WindOverlayStyle::default(),
        );
        (
            layout,
            PressureCrossSectionArtifact {
                section,
                style: CrossSectionStyle::new(CrossSectionProduct::Temperature),
                wind_overlay,
            },
        )
    }

    #[test]
    fn supported_product_list_matches_current_pressure_section_lane() {
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::Temperature
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::RelativeHumidity
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::SpecificHumidity
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::ThetaE
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::WindSpeed
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::WetBulb
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::VaporPressureDeficit
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::DewpointDepression
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::MoistureTransport
        ));
        assert!(supports_pressure_cross_section_product(
            CrossSectionProduct::FireWeather
        ));
        assert!(!supports_pressure_cross_section_product(
            CrossSectionProduct::Omega
        ));
        assert!(!supports_pressure_cross_section_product(
            CrossSectionProduct::Smoke
        ));
    }

    #[test]
    fn pressure_cross_section_facts_capture_route_and_extrema_metadata() {
        let surface = sample_surface_fields();
        let pressure = sample_pressure_fields();
        let layout = sample_layout();
        let artifact = build_pressure_cross_section_from_parts_profiled(
            &surface,
            &pressure,
            ModelId::Hrrr,
            SourceId::Nomads,
            &CycleSpec::new("20260414", 23).unwrap(),
            0,
            &layout,
            CrossSectionProduct::Temperature,
        )
        .unwrap()
        .artifact;

        let facts = summarize_pressure_cross_section_artifact(&layout, &artifact);

        assert_eq!(facts.route.sample_count, 3);
        assert_eq!(facts.route.start.sample_index, 0);
        assert_eq!(facts.route.midpoint.sample_index, 1);
        assert_eq!(facts.route.end.sample_index, 2);
        assert!(facts.route.total_distance_km > 100.0);
        assert_eq!(facts.metadata.field_name.as_deref(), Some("temperature"));
        assert_eq!(facts.metadata.field_units.as_deref(), Some("C"));
        assert_eq!(
            facts
                .metadata
                .attributes
                .get("route_label")
                .map(String::as_str),
            Some("TEST ROUTE")
        );
        assert_eq!(facts.scalar.vertical_kind, "pressure");
        assert_eq!(facts.scalar.vertical_units, "hpa");
        assert_eq!(facts.scalar.level_count, 2);
        assert!(facts.global_minimum().is_some());
        assert!(facts.global_maximum().is_some());
        assert!(facts.global_maximum().unwrap().value >= facts.global_minimum().unwrap().value);
        assert!(facts.lowest_visible_level_minimum().is_some());
        assert!(facts.lowest_visible_level_maximum().is_some());
        let terrain = facts.terrain.as_ref().expect("terrain facts should exist");
        assert!(terrain.surface_pressure_minimum().is_some());
        assert!(terrain.surface_height_maximum_m().is_some());
        assert_eq!(facts.wind.units, "m/s");
        assert!(facts.strongest_wind_speed().is_some());
    }

    #[test]
    fn pressure_cross_section_facts_ignore_below_ground_extrema() {
        let (layout, artifact) = below_ground_extrema_fixture();

        let facts = PressureCrossSectionFacts::from_artifact(&layout, &artifact);

        assert_eq!(facts.global_minimum().unwrap().value, 10.0);
        assert_eq!(facts.global_maximum().unwrap().value, 20.0);
        assert_eq!(facts.lowest_visible_level_maximum().unwrap().value, 20.0);
        assert_eq!(facts.wind.strongest_speed().unwrap().value, 7.0);
        assert_eq!(
            facts
                .terrain
                .as_ref()
                .and_then(|terrain| terrain.surface_pressure_minimum())
                .unwrap()
                .value,
            950.0
        );
    }

    #[test]
    fn pressure_cross_section_builder_returns_finite_theta_e_and_wind_overlay() {
        let surface = sample_surface_fields();
        let pressure = sample_pressure_fields();
        let layout = sample_layout();

        let artifact = build_pressure_cross_section_from_parts_profiled(
            &surface,
            &pressure,
            ModelId::Hrrr,
            SourceId::Nomads,
            &CycleSpec::new("20260414", 23).unwrap(),
            0,
            &layout,
            CrossSectionProduct::ThetaE,
        )
        .unwrap()
        .artifact;

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
    fn pressure_cross_section_builder_returns_finite_moisture_transport() {
        let surface = sample_surface_fields();
        let pressure = sample_pressure_fields();
        let layout = sample_layout();

        let artifact = build_pressure_cross_section_from_parts_profiled(
            &surface,
            &pressure,
            ModelId::Hrrr,
            SourceId::Nomads,
            &CycleSpec::new("20260414", 23).unwrap(),
            0,
            &layout,
            CrossSectionProduct::MoistureTransport,
        )
        .unwrap()
        .artifact;

        assert_eq!(
            artifact.style.product(),
            CrossSectionProduct::MoistureTransport
        );
        assert_eq!(
            artifact.section.metadata().attribute("product_key"),
            Some("moisture_transport")
        );
        assert_eq!(
            artifact.section.metadata().field_units.as_deref(),
            Some("g*m/kg/s")
        );
        assert!(
            artifact
                .section
                .values()
                .iter()
                .all(|value| value.is_finite() && *value > 0.0)
        );
        assert_eq!(artifact.wind_overlay.grid.n_levels(), 2);
        assert_eq!(artifact.wind_overlay.grid.n_points(), 3);
    }

    #[test]
    fn wind_speed_sections_are_converted_to_knots() {
        let pressure_hpa = [1000.0];
        let temperature_c = [20.0];
        let mixing_ratio_kgkg = [0.010];
        let u_ms = [10.0];
        let v_ms = [0.0];
        let values = build_pressure_cross_section_product_values(
            CrossSectionProduct::WindSpeed,
            PressureCrossSectionProductInputs {
                pressure_hpa: &pressure_hpa,
                temperature_c: &temperature_c,
                mixing_ratio_kgkg: &mixing_ratio_kgkg,
                u_ms: &u_ms,
                v_ms: &v_ms,
                optional: PressureCrossSectionOptionalProductFields::default(),
            },
        )
        .unwrap();

        assert_eq!(values.len(), 1);
        assert!((values[0] - 19.438_444_924_406_05).abs() < 1.0e-6);
    }

    #[test]
    fn specific_humidity_sections_convert_mixing_ratio_to_g_per_kg() {
        let pressure_hpa = [1000.0];
        let temperature_c = [20.0];
        let mixing_ratio_kgkg = [0.010];
        let u_ms = [0.0];
        let v_ms = [0.0];
        let values = build_pressure_cross_section_product_values(
            CrossSectionProduct::SpecificHumidity,
            PressureCrossSectionProductInputs {
                pressure_hpa: &pressure_hpa,
                temperature_c: &temperature_c,
                mixing_ratio_kgkg: &mixing_ratio_kgkg,
                u_ms: &u_ms,
                v_ms: &v_ms,
                optional: PressureCrossSectionOptionalProductFields::default(),
            },
        )
        .unwrap();

        assert_eq!(values.len(), 1);
        assert!((values[0] - 9.900_990_099_009_901).abs() < 1.0e-9);
    }

    #[test]
    fn moisture_and_fire_products_use_shared_pressure_inputs_consistently() {
        let pressure_hpa = [1000.0];
        let temperature_c = [20.0];
        let mixing_ratio_kgkg = [0.010];
        let u_ms = [6.0];
        let v_ms = [8.0];
        let inputs = PressureCrossSectionProductInputs {
            pressure_hpa: &pressure_hpa,
            temperature_c: &temperature_c,
            mixing_ratio_kgkg: &mixing_ratio_kgkg,
            u_ms: &u_ms,
            v_ms: &v_ms,
            optional: PressureCrossSectionOptionalProductFields::default(),
        };

        let relative_humidity = build_pressure_cross_section_product_values(
            CrossSectionProduct::RelativeHumidity,
            inputs,
        )
        .unwrap();
        let wet_bulb =
            build_pressure_cross_section_product_values(CrossSectionProduct::WetBulb, inputs)
                .unwrap();
        let vapor_pressure_deficit = build_pressure_cross_section_product_values(
            CrossSectionProduct::VaporPressureDeficit,
            inputs,
        )
        .unwrap();
        let dewpoint_depression = build_pressure_cross_section_product_values(
            CrossSectionProduct::DewpointDepression,
            inputs,
        )
        .unwrap();
        let moisture_transport = build_pressure_cross_section_product_values(
            CrossSectionProduct::MoistureTransport,
            inputs,
        )
        .unwrap();
        let fire_weather =
            build_pressure_cross_section_product_values(CrossSectionProduct::FireWeather, inputs)
                .unwrap();

        let expected_dewpoint_c =
            compute_dewpoint_from_pressure_and_mixing_ratio(&pressure_hpa, &mixing_ratio_kgkg)
                .unwrap();
        let expected_specific_humidity_gkg =
            mixing_ratio_to_specific_humidity_gkg(&mixing_ratio_kgkg);
        let expected_wind_speed_ms = compute_wind_speed_ms(&u_ms, &v_ms).unwrap();

        assert_eq!(fire_weather, relative_humidity);
        assert!(
            (wet_bulb[0]
                - approximate_wet_bulb_temperature_c(temperature_c[0], relative_humidity[0]))
            .abs()
                < 1.0e-9
        );
        assert!(
            (vapor_pressure_deficit[0]
                - tetens_saturation_vapor_pressure_hpa(temperature_c[0])
                    * (1.0 - (relative_humidity[0] / 100.0).clamp(0.0, 1.0)))
            .abs()
                < 1.0e-9
        );
        assert!(
            (dewpoint_depression[0] - (temperature_c[0] - expected_dewpoint_c[0])).abs() < 1.0e-9
        );
        assert!(
            (moisture_transport[0] - expected_specific_humidity_gkg[0] * expected_wind_speed_ms[0])
                .abs()
                < 1.0e-9
        );
    }

    #[test]
    fn omega_and_smoke_products_require_optional_upstream_inputs() {
        let pressure_hpa = [1000.0];
        let temperature_c = [20.0];
        let mixing_ratio_kgkg = [0.010];
        let u_ms = [5.0];
        let v_ms = [0.0];
        let inputs = PressureCrossSectionProductInputs {
            pressure_hpa: &pressure_hpa,
            temperature_c: &temperature_c,
            mixing_ratio_kgkg: &mixing_ratio_kgkg,
            u_ms: &u_ms,
            v_ms: &v_ms,
            optional: PressureCrossSectionOptionalProductFields::default(),
        };

        let omega_err =
            build_pressure_cross_section_product_values(CrossSectionProduct::Omega, inputs)
                .unwrap_err();
        let smoke_err =
            build_pressure_cross_section_product_values(CrossSectionProduct::Smoke, inputs)
                .unwrap_err();

        assert!(
            omega_err
                .to_string()
                .contains("requires sampled omega input")
        );
        assert!(
            smoke_err
                .to_string()
                .contains("requires sampled smoke input")
        );

        let optional_inputs = PressureCrossSectionProductInputs {
            optional: PressureCrossSectionOptionalProductFields {
                omega_pa_s: Some(&[0.5]),
                smoke_ugm3: Some(&[12.0]),
            },
            ..inputs
        };
        let omega = build_pressure_cross_section_product_values(
            CrossSectionProduct::Omega,
            optional_inputs,
        )
        .unwrap();
        let smoke = build_pressure_cross_section_product_values(
            CrossSectionProduct::Smoke,
            optional_inputs,
        )
        .unwrap();

        assert_eq!(omega, vec![18.0]);
        assert_eq!(smoke, vec![12.0]);
    }

    #[test]
    fn sample_stencil_keeps_four_best_candidates_in_distance_order() {
        let surface = SurfaceFields {
            lat: vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0],
            lon: vec![-101.0, -100.0, -99.0, -101.0, -100.0, -99.0],
            nx: 3,
            ny: 2,
            projection: None,
            psfc_pa: vec![100000.0; 6],
            orog_m: vec![0.0; 6],
            orog_is_proxy: false,
            t2_k: vec![290.0; 6],
            q2_kgkg: vec![0.010; 6],
            u10_ms: vec![5.0; 6],
            v10_ms: vec![2.0; 6],
        };
        let point = GeoPoint::new(35.2, -100.1).unwrap();
        let stencil = sample_stencil_for_point(
            &surface,
            &[0usize, 1, 2, 3, 4, 5],
            point,
            HorizontalInterpolation::Bilinear,
        );

        assert_eq!(stencil.len, 4);
        assert_eq!(stencil.indices[0], 1);
        assert!(
            stencil.weights[..stencil.len as usize]
                .iter()
                .all(|weight| weight.is_finite() && *weight > 0.0)
        );
        let weight_sum = stencil.weights[..stencil.len as usize].iter().sum::<f64>();
        assert!((weight_sum - 1.0).abs() < 1.0e-9);
    }
}
