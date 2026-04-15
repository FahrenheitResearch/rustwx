use rustwx_core::GridShape;

use crate::ecape::{EcapeVolumeInputs, SurfaceInputs, VolumeShape, validate_inputs, validate_len};
use crate::error::CalcError;

#[derive(Debug, Clone, Copy)]
pub struct WindGridInputs<'a> {
    pub shape: VolumeShape,
    pub u_3d_ms: &'a [f64],
    pub v_3d_ms: &'a [f64],
    pub height_agl_3d_m: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct FixedStpInputs<'a> {
    pub grid: GridShape,
    pub sbcape_jkg: &'a [f64],
    pub lcl_m: &'a [f64],
    pub srh_1km_m2s2: &'a [f64],
    pub shear_6km_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct EffectiveStpInputs<'a> {
    pub grid: GridShape,
    pub mlcape_jkg: &'a [f64],
    pub mlcin_jkg: &'a [f64],
    pub ml_lcl_m: &'a [f64],
    pub effective_srh_m2s2: &'a [f64],
    pub effective_bulk_wind_difference_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct EffectiveScpInputs<'a> {
    pub grid: GridShape,
    pub mucape_jkg: &'a [f64],
    pub effective_srh_m2s2: &'a [f64],
    pub effective_bulk_wind_difference_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct EffectiveSevereInputs<'a> {
    pub grid: GridShape,
    pub mlcape_jkg: &'a [f64],
    pub mlcin_jkg: &'a [f64],
    pub ml_lcl_m: &'a [f64],
    pub mucape_jkg: &'a [f64],
    pub effective_srh_m2s2: &'a [f64],
    pub effective_bulk_wind_difference_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct ScpEhiInputs<'a> {
    pub grid: GridShape,
    pub scp_cape_jkg: &'a [f64],
    pub scp_srh_m2s2: &'a [f64],
    pub scp_bulk_wind_difference_ms: &'a [f64],
    pub ehi_cape_jkg: &'a [f64],
    pub ehi_srh_m2s2: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct ShipInputs<'a> {
    pub grid: GridShape,
    pub mucape_jkg: &'a [f64],
    pub shear_6km_ms: &'a [f64],
    pub temperature_500c: &'a [f64],
    pub lapse_rate_700_500_cpkm: &'a [f64],
    pub mixing_ratio_500_gkg: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct BulkRichardsonInputs<'a> {
    pub grid: GridShape,
    pub cape_jkg: &'a [f64],
    pub brn_shear_ms: &'a [f64],
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapeCinOutputs {
    pub cape_jkg: Vec<f64>,
    pub cin_jkg: Vec<f64>,
    pub lcl_m: Vec<f64>,
    pub lfc_m: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveSevereOutputs {
    pub stp_effective: Vec<f64>,
    pub scp_effective: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScpEhiOutputs {
    pub scp: Vec<f64>,
    pub ehi: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupportedSevereFields {
    pub sbcape_jkg: Vec<f64>,
    pub mlcin_jkg: Vec<f64>,
    pub mucape_jkg: Vec<f64>,
    pub srh_01km_m2s2: Vec<f64>,
    pub srh_03km_m2s2: Vec<f64>,
    pub shear_06km_ms: Vec<f64>,
    pub stp_fixed: Vec<f64>,
    pub scp_mu_03km_06km_proxy: Vec<f64>,
    pub ehi_sb_01km_proxy: Vec<f64>,
}

pub fn compute_cape_cin(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
    parcel_type: &str,
    top_m: Option<f64>,
) -> Result<CapeCinOutputs, CalcError> {
    validate_inputs(grid, volume, surface)?;
    let (cape, cin, lcl, lfc) = metrust::calc::severe::grid::compute_cape_cin(
        volume.pressure_pa,
        volume.temperature_c,
        volume.qvapor_kgkg,
        volume.height_agl_m,
        surface.psfc_pa,
        surface.t2_k,
        surface.q2_kgkg,
        grid.nx,
        grid.ny,
        volume.nz,
        parcel_type,
        top_m,
    );
    Ok(CapeCinOutputs {
        cape_jkg: cape,
        cin_jkg: cin,
        lcl_m: lcl,
        lfc_m: lfc,
    })
}

pub fn compute_srh(wind: WindGridInputs<'_>, top_m: f64) -> Result<Vec<f64>, CalcError> {
    validate_wind_inputs(wind)?;
    Ok(metrust::calc::severe::grid::compute_srh(
        wind.u_3d_ms,
        wind.v_3d_ms,
        wind.height_agl_3d_m,
        wind.shape.grid.nx,
        wind.shape.grid.ny,
        wind.shape.nz,
        top_m,
    ))
}

pub fn compute_shear(
    wind: WindGridInputs<'_>,
    bottom_m: f64,
    top_m: f64,
) -> Result<Vec<f64>, CalcError> {
    validate_wind_inputs(wind)?;
    Ok(metrust::calc::severe::grid::compute_shear(
        wind.u_3d_ms,
        wind.v_3d_ms,
        wind.height_agl_3d_m,
        wind.shape.grid.nx,
        wind.shape.grid.ny,
        wind.shape.nz,
        bottom_m,
        top_m,
    ))
}

/// Compute fixed-layer STP from precomputed surface-based CAPE, LCL, 0-1 km SRH,
/// and 0-6 km bulk shear grids.
///
/// This follows the operational Thompson-style gates used in the local
/// `wrf-rust-plots` implementation: LCL is capped at 1.0 for values at or below
/// 1000 m, shear is zeroed below 12.5 m/s, and the shear term is capped at 1.5
/// once 0-6 km shear reaches 30 m/s.
pub fn compute_stp_fixed(inputs: FixedStpInputs<'_>) -> Result<Vec<f64>, CalcError> {
    validate_fixed_stp_inputs(inputs)?;
    Ok(inputs
        .sbcape_jkg
        .iter()
        .zip(inputs.lcl_m.iter())
        .zip(inputs.srh_1km_m2s2.iter())
        .zip(inputs.shear_6km_ms.iter())
        .map(|(((cape, lcl), srh), shear)| fixed_stp_value(*cape, *lcl, *srh, *shear))
        .collect())
}

/// Compatibility wrapper for fixed-layer STP.
pub fn compute_stp(
    grid: GridShape,
    sbcape_jkg: &[f64],
    lcl_m: &[f64],
    srh_1km_m2s2: &[f64],
    shear_6km_ms: &[f64],
) -> Result<Vec<f64>, CalcError> {
    compute_stp_fixed(FixedStpInputs {
        grid,
        sbcape_jkg,
        lcl_m,
        srh_1km_m2s2,
        shear_6km_ms,
    })
}

/// Compute effective-layer STP from precomputed mixed-layer parcel and
/// effective-layer kinematic ingredient grids.
///
/// This function intentionally does not derive the effective inflow layer. Callers
/// must provide mixed-layer CAPE/CIN/LCL together with effective SRH and
/// effective bulk wind difference from a profile-aware workflow.
pub fn compute_stp_effective(inputs: EffectiveStpInputs<'_>) -> Result<Vec<f64>, CalcError> {
    validate_effective_stp_inputs(inputs)?;
    Ok(inputs
        .mlcape_jkg
        .iter()
        .zip(inputs.mlcin_jkg.iter())
        .zip(inputs.ml_lcl_m.iter())
        .zip(inputs.effective_srh_m2s2.iter())
        .zip(inputs.effective_bulk_wind_difference_ms.iter())
        .map(|((((cape, cin), lcl), srh), ebwd)| {
            effective_stp_value(*cape, *cin, *lcl, *srh, *ebwd)
        })
        .collect())
}

/// Compute effective-layer STP and SCP together from shared effective-layer
/// kinematic inputs.
///
/// This is intended for callers that already cache effective SRH and effective
/// bulk wind difference upstream and want both high-value effective composites
/// in a single validation and loop pass. Effective inflow-layer derivation and
/// parcel extraction remain upstream/profile-aware responsibilities.
pub fn compute_effective_severe(
    inputs: EffectiveSevereInputs<'_>,
) -> Result<EffectiveSevereOutputs, CalcError> {
    validate_effective_severe_inputs(inputs)?;

    let n = inputs.grid.len();
    let mut stp_effective = Vec::with_capacity(n);
    let mut scp_effective = Vec::with_capacity(n);

    for idx in 0..n {
        let effective_srh = inputs.effective_srh_m2s2[idx];
        let effective_bulk_wind_difference = inputs.effective_bulk_wind_difference_ms[idx];
        stp_effective.push(effective_stp_value(
            inputs.mlcape_jkg[idx],
            inputs.mlcin_jkg[idx],
            inputs.ml_lcl_m[idx],
            effective_srh,
            effective_bulk_wind_difference,
        ));
        scp_effective.push(scp_effective_value(
            inputs.mucape_jkg[idx],
            effective_srh,
            effective_bulk_wind_difference,
        ));
    }

    Ok(EffectiveSevereOutputs {
        stp_effective,
        scp_effective,
    })
}

pub fn compute_ehi(grid: GridShape, cape_jkg: &[f64], srh: &[f64]) -> Result<Vec<f64>, CalcError> {
    validate_grid_fields(grid, &[("cape_jkg", cape_jkg), ("srh", srh)])?;
    Ok(cape_jkg
        .iter()
        .zip(srh.iter())
        .map(|(cape, srh)| ehi_value(*cape, *srh))
        .collect())
}

/// Compute effective-layer SCP from precomputed most-unstable CAPE, effective
/// SRH, and effective bulk wind difference grids.
///
/// This mirrors the local `wrf-rust-plots` gridded SCP behavior. The effective
/// bulk wind difference term is zero below 10 m/s and capped at 1.0 once EBWD
/// reaches 20 m/s.
pub fn compute_scp_effective(inputs: EffectiveScpInputs<'_>) -> Result<Vec<f64>, CalcError> {
    validate_effective_scp_inputs(inputs)?;
    Ok(inputs
        .mucape_jkg
        .iter()
        .zip(inputs.effective_srh_m2s2.iter())
        .zip(inputs.effective_bulk_wind_difference_ms.iter())
        .map(|((cape, srh), ebwd)| scp_effective_value(*cape, *srh, *ebwd))
        .collect())
}

/// Compatibility wrapper for effective-layer SCP ingredients.
pub fn compute_scp(
    grid: GridShape,
    mucape_jkg: &[f64],
    effective_srh_m2s2: &[f64],
    effective_bulk_wind_difference_ms: &[f64],
) -> Result<Vec<f64>, CalcError> {
    compute_scp_effective(EffectiveScpInputs {
        grid,
        mucape_jkg,
        effective_srh_m2s2,
        effective_bulk_wind_difference_ms,
    })
}

/// Compute SCP and EHI together from precomputed grids.
///
/// This helper is intentionally agnostic about parcel type and SRH depth. It is
/// useful for proof and render flows that already cache CAPE, SRH, and bulk-wind
/// grids once and want paired SCP/EHI outputs without repeated validation or
/// call-site wiring.
pub fn compute_scp_ehi(inputs: ScpEhiInputs<'_>) -> Result<ScpEhiOutputs, CalcError> {
    validate_scp_ehi_inputs(inputs)?;

    let n = inputs.grid.len();
    let mut scp = Vec::with_capacity(n);
    let mut ehi = Vec::with_capacity(n);

    for idx in 0..n {
        scp.push(scp_effective_value(
            inputs.scp_cape_jkg[idx],
            inputs.scp_srh_m2s2[idx],
            inputs.scp_bulk_wind_difference_ms[idx],
        ));
        ehi.push(ehi_value(
            inputs.ehi_cape_jkg[idx],
            inputs.ehi_srh_m2s2[idx],
        ));
    }

    Ok(ScpEhiOutputs { scp, ehi })
}

/// Compute the current local `wrf-rust` SHIP-style hail proxy from
/// precomputed most-unstable parcel, 500 hPa, and 700-500 hPa ingredient
/// grids.
///
/// This mirrors the local `wrf-rust` component math, including the SPC-style
/// reduction when MUCAPE is below 1300 J/kg. It intentionally does not derive
/// the 500 hPa temperature/mixing ratio or the 700-500 hPa lapse rate from
/// profiles; callers must provide those upstream. This should not be treated
/// as a canonical SHARPpy-style SHIP implementation yet.
pub fn compute_ship(inputs: ShipInputs<'_>) -> Result<Vec<f64>, CalcError> {
    validate_ship_inputs(inputs)?;
    Ok((0..inputs.grid.len())
        .map(|idx| {
            ship_value(
                inputs.mucape_jkg[idx],
                inputs.shear_6km_ms[idx],
                inputs.temperature_500c[idx],
                inputs.lapse_rate_700_500_cpkm[idx],
                inputs.mixing_ratio_500_gkg[idx],
            )
        })
        .collect())
}

/// Compute Bulk Richardson Number Index (BRI) from CAPE and BRN-shear grids.
///
/// The `brn_shear_ms` input must be the BRN-shear magnitude used by the local
/// `wrf-rust` product: the vector difference between the 0-500 m mean wind and
/// the 0-6 km mean wind. This is not interchangeable with plain 0-6 km bulk
/// shear. Degenerate denominators are zero-filled to match local gridded
/// behavior.
pub fn compute_bri(inputs: BulkRichardsonInputs<'_>) -> Result<Vec<f64>, CalcError> {
    validate_bulk_richardson_inputs(inputs)?;
    Ok((0..inputs.grid.len())
        .map(|idx| bri_value(inputs.cape_jkg[idx], inputs.brn_shear_ms[idx]))
        .collect())
}

/// Compute the currently supported gridded severe bundle without inventing
/// effective-layer derivation.
///
/// This bundle is intentionally conservative:
/// - `stp_fixed` uses the fixed-layer Thompson-style formula with `sbCAPE`,
///   `sbLCL`, `0-1 km SRH`, and `0-6 km bulk shear`
/// - `scp_mu_03km_06km_proxy` uses `muCAPE` with `0-3 km SRH` and `0-6 km bulk
///   shear` through the existing SCP wrapper, but is still a fixed-depth proxy,
///   not an effective-layer SCP
/// - `ehi_sb_01km_proxy` uses `sbCAPE` with `0-1 km SRH`
///
/// This is suitable for proof plots and for honest operational use where the
/// effective inflow layer has not yet been derived upstream. It does not claim
/// to be full effective-layer severe diagnostics.
pub fn compute_supported_severe_fields(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
) -> Result<SupportedSevereFields, CalcError> {
    validate_inputs(grid, volume, surface)?;

    let sb = compute_cape_cin(grid, volume, surface, "sb", None)?;
    let ml = compute_cape_cin(grid, volume, surface, "ml", None)?;
    let mu = compute_cape_cin(grid, volume, surface, "mu", None)?;

    let wind = WindGridInputs {
        shape: VolumeShape::new(grid, volume.nz)?,
        u_3d_ms: volume.u_ms,
        v_3d_ms: volume.v_ms,
        height_agl_3d_m: volume.height_agl_m,
    };
    let srh_01km_m2s2 = compute_srh(wind, 1000.0)?;
    let srh_03km_m2s2 = compute_srh(wind, 3000.0)?;
    let shear_06km_ms = compute_shear(wind, 0.0, 6000.0)?;
    let stp_fixed = compute_stp_fixed(FixedStpInputs {
        grid,
        sbcape_jkg: &sb.cape_jkg,
        lcl_m: &sb.lcl_m,
        srh_1km_m2s2: &srh_01km_m2s2,
        shear_6km_ms: &shear_06km_ms,
    })?;
    let scp_ehi = compute_scp_ehi(ScpEhiInputs {
        grid,
        scp_cape_jkg: &mu.cape_jkg,
        scp_srh_m2s2: &srh_03km_m2s2,
        scp_bulk_wind_difference_ms: &shear_06km_ms,
        ehi_cape_jkg: &sb.cape_jkg,
        ehi_srh_m2s2: &srh_01km_m2s2,
    })?;

    Ok(SupportedSevereFields {
        sbcape_jkg: sb.cape_jkg,
        mlcin_jkg: ml.cin_jkg,
        mucape_jkg: mu.cape_jkg,
        srh_01km_m2s2,
        srh_03km_m2s2,
        shear_06km_ms,
        stp_fixed,
        scp_mu_03km_06km_proxy: scp_ehi.scp,
        ehi_sb_01km_proxy: scp_ehi.ehi,
    })
}

pub use metrust::calc::severe::critical_angle;
pub use metrust::calc::severe::significant_tornado_parameter;
pub use metrust::calc::severe::supercell_composite_parameter;

fn fixed_stp_value(sbcape_jkg: f64, lcl_m: f64, srh_1km_m2s2: f64, shear_6km_ms: f64) -> f64 {
    let cape_term = (sbcape_jkg / 1500.0).max(0.0);
    let lcl_term = if lcl_m >= 2000.0 {
        0.0
    } else if lcl_m <= 1000.0 {
        1.0
    } else {
        (2000.0 - lcl_m) / 1000.0
    };
    let srh_term = (srh_1km_m2s2 / 150.0).max(0.0);
    let shear_term = if shear_6km_ms < 12.5 {
        0.0
    } else if shear_6km_ms >= 30.0 {
        1.5
    } else {
        shear_6km_ms / 20.0
    };

    cape_term * lcl_term * srh_term * shear_term
}

fn effective_stp_value(
    mlcape_jkg: f64,
    mlcin_jkg: f64,
    ml_lcl_m: f64,
    effective_srh_m2s2: f64,
    effective_bulk_wind_difference_ms: f64,
) -> f64 {
    let cape_term = (mlcape_jkg / 1500.0).max(0.0);
    let lcl_term = if ml_lcl_m >= 2000.0 {
        0.0
    } else if ml_lcl_m <= 1000.0 {
        1.0
    } else {
        (2000.0 - ml_lcl_m) / 1000.0
    };
    let srh_term = (effective_srh_m2s2 / 150.0).max(0.0);
    let shear_term = if effective_bulk_wind_difference_ms < 12.5 {
        0.0
    } else if effective_bulk_wind_difference_ms >= 30.0 {
        1.5
    } else {
        effective_bulk_wind_difference_ms / 20.0
    };
    let cin_term = ((200.0 + mlcin_jkg) / 150.0).clamp(0.0, 1.0);

    cape_term * lcl_term * srh_term * shear_term * cin_term
}

fn ehi_value(cape_jkg: f64, srh_m2s2: f64) -> f64 {
    (cape_jkg * srh_m2s2) / 160000.0
}

fn scp_effective_value(
    mucape_jkg: f64,
    effective_srh_m2s2: f64,
    effective_bulk_wind_difference_ms: f64,
) -> f64 {
    let cape_term = (mucape_jkg / 1000.0).max(0.0);
    let srh_term = (effective_srh_m2s2 / 50.0).max(0.0);
    let shear_term = if effective_bulk_wind_difference_ms > 20.0 {
        1.0
    } else if effective_bulk_wind_difference_ms < 10.0 {
        0.0
    } else {
        effective_bulk_wind_difference_ms / 20.0
    };

    cape_term * srh_term * shear_term
}

fn ship_value(
    mucape_jkg: f64,
    shear_6km_ms: f64,
    temperature_500c: f64,
    lapse_rate_700_500_cpkm: f64,
    mixing_ratio_500_gkg: f64,
) -> f64 {
    let mucape = mucape_jkg.max(0.0);
    let shear = shear_6km_ms.max(0.0);
    let temperature_500_term = (-temperature_500c).max(0.0);
    let lapse_rate = lapse_rate_700_500_cpkm.max(0.0);
    let mixing_ratio = mixing_ratio_500_gkg.max(0.0);

    let ship = (mucape * mixing_ratio * lapse_rate * temperature_500_term * shear) / 42_000_000.0;

    if mucape < 1300.0 {
        ship * (mucape / 1300.0)
    } else {
        ship
    }
}

fn bri_value(cape_jkg: f64, brn_shear_ms: f64) -> f64 {
    let denom = 0.5 * brn_shear_ms * brn_shear_ms;
    if denom > 0.1 {
        cape_jkg.max(0.0) / denom
    } else {
        0.0
    }
}

fn validate_wind_inputs(wind: WindGridInputs<'_>) -> Result<(), CalcError> {
    let n3d = wind.shape.len3d();
    validate_len("u_3d_ms", wind.u_3d_ms.len(), n3d)?;
    validate_len("v_3d_ms", wind.v_3d_ms.len(), n3d)?;
    validate_len("height_agl_3d_m", wind.height_agl_3d_m.len(), n3d)?;
    Ok(())
}

fn validate_grid_fields(
    grid: GridShape,
    fields: &[(&'static str, &[f64])],
) -> Result<(), CalcError> {
    let n = grid.len();
    for (field, values) in fields {
        validate_len(field, values.len(), n)?;
    }
    Ok(())
}

fn validate_fixed_stp_inputs(inputs: FixedStpInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("sbcape_jkg", inputs.sbcape_jkg),
            ("lcl_m", inputs.lcl_m),
            ("srh_1km_m2s2", inputs.srh_1km_m2s2),
            ("shear_6km_ms", inputs.shear_6km_ms),
        ],
    )
}

fn validate_effective_stp_inputs(inputs: EffectiveStpInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("mlcape_jkg", inputs.mlcape_jkg),
            ("mlcin_jkg", inputs.mlcin_jkg),
            ("ml_lcl_m", inputs.ml_lcl_m),
            ("effective_srh_m2s2", inputs.effective_srh_m2s2),
            (
                "effective_bulk_wind_difference_ms",
                inputs.effective_bulk_wind_difference_ms,
            ),
        ],
    )
}

fn validate_effective_scp_inputs(inputs: EffectiveScpInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("mucape_jkg", inputs.mucape_jkg),
            ("effective_srh_m2s2", inputs.effective_srh_m2s2),
            (
                "effective_bulk_wind_difference_ms",
                inputs.effective_bulk_wind_difference_ms,
            ),
        ],
    )
}

fn validate_effective_severe_inputs(inputs: EffectiveSevereInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("mlcape_jkg", inputs.mlcape_jkg),
            ("mlcin_jkg", inputs.mlcin_jkg),
            ("ml_lcl_m", inputs.ml_lcl_m),
            ("mucape_jkg", inputs.mucape_jkg),
            ("effective_srh_m2s2", inputs.effective_srh_m2s2),
            (
                "effective_bulk_wind_difference_ms",
                inputs.effective_bulk_wind_difference_ms,
            ),
        ],
    )
}

fn validate_scp_ehi_inputs(inputs: ScpEhiInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("scp_cape_jkg", inputs.scp_cape_jkg),
            ("scp_srh_m2s2", inputs.scp_srh_m2s2),
            (
                "scp_bulk_wind_difference_ms",
                inputs.scp_bulk_wind_difference_ms,
            ),
            ("ehi_cape_jkg", inputs.ehi_cape_jkg),
            ("ehi_srh_m2s2", inputs.ehi_srh_m2s2),
        ],
    )
}

fn validate_ship_inputs(inputs: ShipInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("mucape_jkg", inputs.mucape_jkg),
            ("shear_6km_ms", inputs.shear_6km_ms),
            ("temperature_500c", inputs.temperature_500c),
            ("lapse_rate_700_500_cpkm", inputs.lapse_rate_700_500_cpkm),
            ("mixing_ratio_500_gkg", inputs.mixing_ratio_500_gkg),
        ],
    )
}

fn validate_bulk_richardson_inputs(inputs: BulkRichardsonInputs<'_>) -> Result<(), CalcError> {
    validate_grid_fields(
        inputs.grid,
        &[
            ("cape_jkg", inputs.cape_jkg),
            ("brn_shear_ms", inputs.brn_shear_ms),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1.0e-9,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn fixed_stp_matches_operational_lcl_and_shear_gates() {
        assert_close(fixed_stp_value(1500.0, 500.0, 150.0, 20.0), 1.0);
        assert_close(fixed_stp_value(1500.0, 1000.0, 150.0, 12.0), 0.0);
        assert_close(fixed_stp_value(1500.0, 1000.0, 150.0, 40.0), 1.5);
    }

    #[test]
    fn effective_stp_applies_cin_and_ebwd_limits() {
        assert_close(effective_stp_value(1500.0, -50.0, 1000.0, 150.0, 10.0), 0.0);
        assert_close(effective_stp_value(1500.0, -50.0, 1000.0, 150.0, 20.0), 1.0);
        assert_close(
            effective_stp_value(1500.0, -250.0, 1000.0, 150.0, 20.0),
            0.0,
        );
        assert_close(effective_stp_value(1500.0, -50.0, 1000.0, 150.0, 40.0), 1.5);
    }

    #[test]
    fn effective_scp_uses_ebwd_thresholds() {
        assert_close(scp_effective_value(3000.0, 150.0, 8.0), 0.0);
        assert_close(scp_effective_value(3000.0, 150.0, 20.0), 9.0);
        assert_close(scp_effective_value(3000.0, 150.0, 30.0), 9.0);
    }

    #[test]
    fn effective_severe_bundle_matches_component_formulas() {
        let outputs = compute_effective_severe(EffectiveSevereInputs {
            grid: GridShape::new(4, 1).unwrap(),
            mlcape_jkg: &[1500.0, 1500.0, 1500.0, 1500.0],
            mlcin_jkg: &[-50.0, -50.0, -250.0, -50.0],
            ml_lcl_m: &[1000.0, 1000.0, 1000.0, 1000.0],
            mucape_jkg: &[3000.0, 3000.0, 3000.0, 3000.0],
            effective_srh_m2s2: &[150.0, 150.0, 150.0, 150.0],
            effective_bulk_wind_difference_ms: &[8.0, 20.0, 20.0, 40.0],
        })
        .unwrap();

        assert_eq!(outputs.stp_effective, vec![0.0, 1.0, 0.0, 1.5]);
        assert_eq!(outputs.scp_effective, vec![0.0, 9.0, 9.0, 9.0]);
    }

    #[test]
    fn scp_ehi_bundle_matches_component_formulas() {
        let outputs = compute_scp_ehi(ScpEhiInputs {
            grid: GridShape::new(3, 1).unwrap(),
            scp_cape_jkg: &[3000.0, 3000.0, 3000.0],
            scp_srh_m2s2: &[150.0, 150.0, 150.0],
            scp_bulk_wind_difference_ms: &[8.0, 20.0, 30.0],
            ehi_cape_jkg: &[2000.0, 1600.0, 800.0],
            ehi_srh_m2s2: &[200.0, 100.0, 50.0],
        })
        .unwrap();

        assert_eq!(outputs.scp, vec![0.0, 9.0, 9.0]);
        assert_eq!(outputs.ehi, vec![2.5, 1.0, 0.25]);
    }

    #[test]
    fn ship_matches_local_proxy_formula_and_low_cape_scaling() {
        assert_close(ship_value(2000.0, 20.0, -15.0, 7.0, 10.0), 1.0);
        assert_close(
            ship_value(1000.0, 20.0, -15.0, 7.0, 10.0),
            0.38461538461538464,
        );
        assert_close(ship_value(2000.0, 20.0, 5.0, 7.0, 10.0), 0.0);
    }

    #[test]
    fn bri_uses_brn_shear_and_zeroes_degenerate_denominator() {
        assert_close(bri_value(2000.0, 20.0), 10.0);
        assert_close(bri_value(500.0, 30.0), 1.1111111111111112);
        assert_close(bri_value(1000.0, 0.1), 0.0);
    }

    #[test]
    fn supported_severe_fields_reuse_fixed_and_proxy_component_math() {
        let grid = GridShape::new(1, 1).unwrap();
        let volume = EcapeVolumeInputs {
            pressure_pa: &[95_000.0, 90_000.0, 85_000.0, 70_000.0, 50_000.0, 30_000.0],
            temperature_c: &[26.0, 22.0, 18.0, 8.0, -10.0, -38.0],
            qvapor_kgkg: &[0.016, 0.013, 0.010, 0.005, 0.0015, 0.0003],
            height_agl_m: &[150.0, 800.0, 1500.0, 3000.0, 5600.0, 9200.0],
            u_ms: &[6.0, 9.0, 12.0, 18.0, 26.0, 33.0],
            v_ms: &[2.0, 5.0, 8.0, 13.0, 20.0, 28.0],
            nz: 6,
        };
        let surface = SurfaceInputs {
            psfc_pa: &[100_000.0],
            t2_k: &[303.15],
            q2_kgkg: &[0.018],
            u10_ms: &[5.0],
            v10_ms: &[1.5],
        };

        let supported = compute_supported_severe_fields(grid, volume, surface).unwrap();
        let sb = compute_cape_cin(grid, volume, surface, "sb", None).unwrap();
        let ml = compute_cape_cin(grid, volume, surface, "ml", None).unwrap();
        let mu = compute_cape_cin(grid, volume, surface, "mu", None).unwrap();
        let wind = WindGridInputs {
            shape: VolumeShape::new(grid, volume.nz).unwrap(),
            u_3d_ms: volume.u_ms,
            v_3d_ms: volume.v_ms,
            height_agl_3d_m: volume.height_agl_m,
        };
        let srh_01km = compute_srh(wind, 1000.0).unwrap();
        let srh_03km = compute_srh(wind, 3000.0).unwrap();
        let shear_06km = compute_shear(wind, 0.0, 6000.0).unwrap();
        let stp_fixed = compute_stp_fixed(FixedStpInputs {
            grid,
            sbcape_jkg: &sb.cape_jkg,
            lcl_m: &sb.lcl_m,
            srh_1km_m2s2: &srh_01km,
            shear_6km_ms: &shear_06km,
        })
        .unwrap();
        let proxy = compute_scp_ehi(ScpEhiInputs {
            grid,
            scp_cape_jkg: &mu.cape_jkg,
            scp_srh_m2s2: &srh_03km,
            scp_bulk_wind_difference_ms: &shear_06km,
            ehi_cape_jkg: &sb.cape_jkg,
            ehi_srh_m2s2: &srh_01km,
        })
        .unwrap();

        assert_eq!(supported.sbcape_jkg, sb.cape_jkg);
        assert_eq!(supported.mlcin_jkg, ml.cin_jkg);
        assert_eq!(supported.mucape_jkg, mu.cape_jkg);
        assert_eq!(supported.srh_01km_m2s2, srh_01km);
        assert_eq!(supported.srh_03km_m2s2, srh_03km);
        assert_eq!(supported.shear_06km_ms, shear_06km);
        assert_eq!(supported.stp_fixed, stp_fixed);
        assert_eq!(supported.scp_mu_03km_06km_proxy, proxy.scp);
        assert_eq!(supported.ehi_sb_01km_proxy, proxy.ehi);
    }
}
