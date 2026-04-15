use rustwx_core::GridShape;

use crate::error::CalcError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeShape {
    pub grid: GridShape,
    pub nz: usize,
}

impl VolumeShape {
    pub fn new(grid: GridShape, nz: usize) -> Result<Self, CalcError> {
        if nz == 0 {
            return Err(CalcError::LengthMismatch {
                field: "nz",
                expected: 1,
                actual: 0,
            });
        }
        Ok(Self { grid, nz })
    }

    pub fn len2d(self) -> usize {
        self.grid.len()
    }

    pub fn len3d(self) -> usize {
        self.grid.len() * self.nz
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EcapeGridInputs<'a> {
    pub shape: VolumeShape,
    pub pressure_3d_pa: &'a [f64],
    pub temperature_3d_c: &'a [f64],
    pub qvapor_3d_kgkg: &'a [f64],
    pub height_agl_3d_m: &'a [f64],
    pub u_3d_ms: &'a [f64],
    pub v_3d_ms: &'a [f64],
    pub psfc_pa: &'a [f64],
    pub t2_k: &'a [f64],
    pub q2_kgkg: &'a [f64],
    pub u10_ms: &'a [f64],
    pub v10_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct EcapeVolumeInputs<'a> {
    pub pressure_pa: &'a [f64],
    pub temperature_c: &'a [f64],
    pub qvapor_kgkg: &'a [f64],
    pub height_agl_m: &'a [f64],
    pub u_ms: &'a [f64],
    pub v_ms: &'a [f64],
    pub nz: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct SurfaceInputs<'a> {
    pub psfc_pa: &'a [f64],
    pub t2_k: &'a [f64],
    pub q2_kgkg: &'a [f64],
    pub u10_ms: &'a [f64],
    pub v10_ms: &'a [f64],
}

#[derive(Debug, Clone, Copy)]
pub struct EcapeOptions<'a> {
    pub parcel_type: &'a str,
    pub storm_motion_type: &'a str,
    pub entrainment_rate: Option<f64>,
    pub pseudoadiabatic: Option<bool>,
    pub storm_motion: Option<(f64, f64)>,
}

impl Default for EcapeOptions<'_> {
    fn default() -> Self {
        Self::new("sb", "right_moving")
    }
}

impl<'a> EcapeOptions<'a> {
    pub fn new(parcel_type: &'a str, storm_motion_type: &'a str) -> Self {
        Self {
            parcel_type,
            storm_motion_type,
            entrainment_rate: None,
            pseudoadiabatic: None,
            storm_motion: None,
        }
    }

    pub fn with_entrainment_rate(mut self, entrainment_rate: f64) -> Self {
        self.entrainment_rate = Some(entrainment_rate);
        self
    }

    pub fn with_pseudoadiabatic(mut self, pseudoadiabatic: bool) -> Self {
        self.pseudoadiabatic = Some(pseudoadiabatic);
        self
    }

    pub fn with_user_storm_motion(mut self, storm_u_ms: f64, storm_v_ms: f64) -> Self {
        self.storm_motion = Some((storm_u_ms, storm_v_ms));
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EcapeTripletOptions<'a> {
    pub storm_motion_type: &'a str,
    pub entrainment_rate: Option<f64>,
    pub pseudoadiabatic: Option<bool>,
    pub storm_motion: Option<(f64, f64)>,
}

impl Default for EcapeTripletOptions<'_> {
    fn default() -> Self {
        Self::new("right_moving")
    }
}

impl<'a> EcapeTripletOptions<'a> {
    pub fn new(storm_motion_type: &'a str) -> Self {
        Self {
            storm_motion_type,
            entrainment_rate: None,
            pseudoadiabatic: None,
            storm_motion: None,
        }
    }

    pub fn with_entrainment_rate(mut self, entrainment_rate: f64) -> Self {
        self.entrainment_rate = Some(entrainment_rate);
        self
    }

    pub fn with_pseudoadiabatic(mut self, pseudoadiabatic: bool) -> Self {
        self.pseudoadiabatic = Some(pseudoadiabatic);
        self
    }

    pub fn with_user_storm_motion(mut self, storm_u_ms: f64, storm_v_ms: f64) -> Self {
        self.storm_motion = Some((storm_u_ms, storm_v_ms));
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EcapeFields {
    pub ecape_jkg: Vec<f64>,
    pub ncape_jkg: Vec<f64>,
    pub cape_jkg: Vec<f64>,
    pub cin_jkg: Vec<f64>,
    pub lfc_m: Vec<f64>,
    pub el_m: Vec<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EcapeFieldsWithFailureMask {
    pub fields: EcapeFields,
    pub failure_mask: Vec<u8>,
}

impl EcapeFieldsWithFailureMask {
    pub fn failure_count(&self) -> usize {
        self.failure_mask.iter().filter(|&&flag| flag != 0).count()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EcapeTripletFieldsWithFailureMask {
    pub sb: EcapeFieldsWithFailureMask,
    pub ml: EcapeFieldsWithFailureMask,
    pub mu: EcapeFieldsWithFailureMask,
}

impl EcapeTripletFieldsWithFailureMask {
    pub fn total_failure_count(&self) -> usize {
        self.sb.failure_count() + self.ml.failure_count() + self.mu.failure_count()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EcapeTripletFields {
    pub sb: EcapeFields,
    pub ml: EcapeFields,
    pub mu: EcapeFields,
}

pub fn compute_ecape(
    inputs: EcapeGridInputs<'_>,
    options: &EcapeOptions<'_>,
) -> Result<EcapeFields, CalcError> {
    let (volume, surface) = split_inputs(inputs);
    compute_ecape_from_parts(inputs.shape.grid, volume, surface, *options)
}

pub fn compute_ecape_with_failure_mask(
    inputs: EcapeGridInputs<'_>,
    options: &EcapeOptions<'_>,
) -> Result<EcapeFieldsWithFailureMask, CalcError> {
    let (volume, surface) = split_inputs(inputs);
    compute_ecape_with_failure_mask_from_parts(inputs.shape.grid, volume, surface, *options)
}

pub fn compute_ecape_triplet_with_failure_mask(
    inputs: EcapeGridInputs<'_>,
    options: &EcapeTripletOptions<'_>,
) -> Result<EcapeTripletFieldsWithFailureMask, CalcError> {
    let (volume, surface) = split_inputs(inputs);
    compute_ecape_triplet_with_failure_mask_from_parts(inputs.shape.grid, volume, surface, *options)
}

pub fn compute_ecape_triplet(
    inputs: EcapeGridInputs<'_>,
    options: &EcapeTripletOptions<'_>,
) -> Result<EcapeTripletFields, CalcError> {
    let (volume, surface) = split_inputs(inputs);
    compute_ecape_triplet_from_parts(inputs.shape.grid, volume, surface, *options)
}

pub fn compute_ecape_from_parts(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
    options: EcapeOptions<'_>,
) -> Result<EcapeFields, CalcError> {
    validate_inputs(grid, volume, surface)?;

    let (storm_u, storm_v) = unzip_storm_motion(options);
    let (ecape, ncape, cape, cin, lfc, el) = metrust::calc::severe::grid::compute_ecape(
        volume.pressure_pa,
        volume.temperature_c,
        volume.qvapor_kgkg,
        volume.height_agl_m,
        volume.u_ms,
        volume.v_ms,
        surface.psfc_pa,
        surface.t2_k,
        surface.q2_kgkg,
        surface.u10_ms,
        surface.v10_ms,
        grid.nx,
        grid.ny,
        volume.nz,
        options.parcel_type,
        options.storm_motion_type,
        options.entrainment_rate,
        options.pseudoadiabatic,
        storm_u,
        storm_v,
    )
    .map_err(CalcError::Metrust)?;

    Ok(EcapeFields {
        ecape_jkg: ecape,
        ncape_jkg: ncape,
        cape_jkg: cape,
        cin_jkg: cin,
        lfc_m: lfc,
        el_m: el,
    })
}

pub fn compute_ecape_with_failure_mask_from_parts(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
    options: EcapeOptions<'_>,
) -> Result<EcapeFieldsWithFailureMask, CalcError> {
    validate_inputs(grid, volume, surface)?;

    let (storm_u, storm_v) = unzip_storm_motion(options);
    let (ecape, ncape, cape, cin, lfc, el, failure_mask) =
        metrust::calc::severe::grid::compute_ecape_with_failure_mask(
            volume.pressure_pa,
            volume.temperature_c,
            volume.qvapor_kgkg,
            volume.height_agl_m,
            volume.u_ms,
            volume.v_ms,
            surface.psfc_pa,
            surface.t2_k,
            surface.q2_kgkg,
            surface.u10_ms,
            surface.v10_ms,
            grid.nx,
            grid.ny,
            volume.nz,
            options.parcel_type,
            options.storm_motion_type,
            options.entrainment_rate,
            options.pseudoadiabatic,
            storm_u,
            storm_v,
        )
        .map_err(CalcError::Metrust)?;

    Ok(EcapeFieldsWithFailureMask {
        fields: EcapeFields {
            ecape_jkg: ecape,
            ncape_jkg: ncape,
            cape_jkg: cape,
            cin_jkg: cin,
            lfc_m: lfc,
            el_m: el,
        },
        failure_mask,
    })
}

pub fn compute_ecape_triplet_with_failure_mask_from_parts(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
    options: EcapeTripletOptions<'_>,
) -> Result<EcapeTripletFieldsWithFailureMask, CalcError> {
    validate_inputs(grid, volume, surface)?;

    let (storm_u, storm_v) = unzip_triplet_storm_motion(options);
    let triplet = metrust::calc::severe::grid::compute_ecape_triplet_with_failure_mask(
        volume.pressure_pa,
        volume.temperature_c,
        volume.qvapor_kgkg,
        volume.height_agl_m,
        volume.u_ms,
        volume.v_ms,
        surface.psfc_pa,
        surface.t2_k,
        surface.q2_kgkg,
        surface.u10_ms,
        surface.v10_ms,
        grid.nx,
        grid.ny,
        volume.nz,
        options.storm_motion_type,
        options.entrainment_rate,
        options.pseudoadiabatic,
        storm_u,
        storm_v,
    )
    .map_err(CalcError::Metrust)?;

    Ok(EcapeTripletFieldsWithFailureMask {
        sb: EcapeFieldsWithFailureMask {
            fields: EcapeFields {
                ecape_jkg: triplet.sb.fields.ecape,
                ncape_jkg: triplet.sb.fields.ncape,
                cape_jkg: triplet.sb.fields.cape,
                cin_jkg: triplet.sb.fields.cin,
                lfc_m: triplet.sb.fields.lfc,
                el_m: triplet.sb.fields.el,
            },
            failure_mask: triplet.sb.failure_mask,
        },
        ml: EcapeFieldsWithFailureMask {
            fields: EcapeFields {
                ecape_jkg: triplet.ml.fields.ecape,
                ncape_jkg: triplet.ml.fields.ncape,
                cape_jkg: triplet.ml.fields.cape,
                cin_jkg: triplet.ml.fields.cin,
                lfc_m: triplet.ml.fields.lfc,
                el_m: triplet.ml.fields.el,
            },
            failure_mask: triplet.ml.failure_mask,
        },
        mu: EcapeFieldsWithFailureMask {
            fields: EcapeFields {
                ecape_jkg: triplet.mu.fields.ecape,
                ncape_jkg: triplet.mu.fields.ncape,
                cape_jkg: triplet.mu.fields.cape,
                cin_jkg: triplet.mu.fields.cin,
                lfc_m: triplet.mu.fields.lfc,
                el_m: triplet.mu.fields.el,
            },
            failure_mask: triplet.mu.failure_mask,
        },
    })
}

pub fn compute_ecape_triplet_from_parts(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
    options: EcapeTripletOptions<'_>,
) -> Result<EcapeTripletFields, CalcError> {
    validate_inputs(grid, volume, surface)?;

    let (storm_u, storm_v) = unzip_triplet_storm_motion(options);
    let triplet = metrust::calc::severe::grid::compute_ecape_triplet(
        volume.pressure_pa,
        volume.temperature_c,
        volume.qvapor_kgkg,
        volume.height_agl_m,
        volume.u_ms,
        volume.v_ms,
        surface.psfc_pa,
        surface.t2_k,
        surface.q2_kgkg,
        surface.u10_ms,
        surface.v10_ms,
        grid.nx,
        grid.ny,
        volume.nz,
        options.storm_motion_type,
        options.entrainment_rate,
        options.pseudoadiabatic,
        storm_u,
        storm_v,
    )
    .map_err(CalcError::Metrust)?;

    Ok(EcapeTripletFields {
        sb: EcapeFields {
            ecape_jkg: triplet.sb.ecape,
            ncape_jkg: triplet.sb.ncape,
            cape_jkg: triplet.sb.cape,
            cin_jkg: triplet.sb.cin,
            lfc_m: triplet.sb.lfc,
            el_m: triplet.sb.el,
        },
        ml: EcapeFields {
            ecape_jkg: triplet.ml.ecape,
            ncape_jkg: triplet.ml.ncape,
            cape_jkg: triplet.ml.cape,
            cin_jkg: triplet.ml.cin,
            lfc_m: triplet.ml.lfc,
            el_m: triplet.ml.el,
        },
        mu: EcapeFields {
            ecape_jkg: triplet.mu.ecape,
            ncape_jkg: triplet.mu.ncape,
            cape_jkg: triplet.mu.cape,
            cin_jkg: triplet.mu.cin,
            lfc_m: triplet.mu.lfc,
            el_m: triplet.mu.el,
        },
    })
}

pub(crate) fn validate_inputs(
    grid: GridShape,
    volume: EcapeVolumeInputs<'_>,
    surface: SurfaceInputs<'_>,
) -> Result<(), CalcError> {
    let n2d = grid.len();
    let n3d = n2d * volume.nz;

    validate_len("pressure_pa", volume.pressure_pa.len(), n3d)?;
    validate_len("temperature_c", volume.temperature_c.len(), n3d)?;
    validate_len("qvapor_kgkg", volume.qvapor_kgkg.len(), n3d)?;
    validate_len("height_agl_m", volume.height_agl_m.len(), n3d)?;
    validate_len("u_ms", volume.u_ms.len(), n3d)?;
    validate_len("v_ms", volume.v_ms.len(), n3d)?;

    validate_len("psfc_pa", surface.psfc_pa.len(), n2d)?;
    validate_len("t2_k", surface.t2_k.len(), n2d)?;
    validate_len("q2_kgkg", surface.q2_kgkg.len(), n2d)?;
    validate_len("u10_ms", surface.u10_ms.len(), n2d)?;
    validate_len("v10_ms", surface.v10_ms.len(), n2d)?;

    Ok(())
}

fn split_inputs(inputs: EcapeGridInputs<'_>) -> (EcapeVolumeInputs<'_>, SurfaceInputs<'_>) {
    (
        EcapeVolumeInputs {
            pressure_pa: inputs.pressure_3d_pa,
            temperature_c: inputs.temperature_3d_c,
            qvapor_kgkg: inputs.qvapor_3d_kgkg,
            height_agl_m: inputs.height_agl_3d_m,
            u_ms: inputs.u_3d_ms,
            v_ms: inputs.v_3d_ms,
            nz: inputs.shape.nz,
        },
        SurfaceInputs {
            psfc_pa: inputs.psfc_pa,
            t2_k: inputs.t2_k,
            q2_kgkg: inputs.q2_kgkg,
            u10_ms: inputs.u10_ms,
            v10_ms: inputs.v10_ms,
        },
    )
}

fn unzip_storm_motion(options: EcapeOptions<'_>) -> (Option<f64>, Option<f64>) {
    match options.storm_motion {
        Some((u, v)) => (Some(u), Some(v)),
        None => (None, None),
    }
}

fn unzip_triplet_storm_motion(options: EcapeTripletOptions<'_>) -> (Option<f64>, Option<f64>) {
    match options.storm_motion {
        Some((u, v)) => (Some(u), Some(v)),
        None => (None, None),
    }
}

pub(crate) fn validate_len(
    field: &'static str,
    actual: usize,
    expected: usize,
) -> Result<(), CalcError> {
    if actual == expected {
        Ok(())
    } else {
        Err(CalcError::LengthMismatch {
            field,
            expected,
            actual,
        })
    }
}
