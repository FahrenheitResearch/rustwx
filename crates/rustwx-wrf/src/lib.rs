use ndarray::Axis;
use rayon::prelude::*;
use rustwx_core::{
    CanonicalField, FieldSelector, GridShape, LatLonGrid, SelectedField2D, VerticalSelector,
};
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const HDF5_SIGNATURE: &[u8] = b"\x89HDF\r\n\x1a\n";
const G: f64 = 9.80665;
const G_SLP: f64 = 9.81;
const RD: f64 = 287.058;
const RD_SLP: f64 = 287.0;
const P0: f64 = 100_000.0;
const KAPPA: f64 = 0.2857142857;
const USSALR: f64 = 0.0065;
const PCONST: f64 = 10_000.0;
const TC_SLP: f64 = 273.16 + 17.5;
const OMEGA: f64 = 7.292_115_9e-5;
const CELKEL: f64 = 273.15;
const GAMMA_SEVEN: f64 = 720.0;
const PI: f64 = std::f64::consts::PI;
const EARTH_RADIUS_M: f64 = 6_370_000.0;
const RHOWAT: f64 = 1000.0;
const ALPHA: f64 = 0.224;
const RHO_R: f64 = 1000.0;
const RHO_S: f64 = 100.0;
const RHO_G: f64 = 400.0;
const RN0_R: f64 = 8.0e6;
const RN0_S: f64 = 2.0e7;
const RN0_G: f64 = 4.0e6;

#[derive(Debug, thiserror::Error)]
pub enum WrfError {
    #[error("input did not look like a netcdf4/hdf5 wrfout file")]
    NotWrfInput,
    #[error("netcdf error: {0}")]
    Netcdf(String),
    #[error("missing dimension '{0}'")]
    MissingDimension(String),
    #[error("wrf file does not contain 3-D state (missing 'bottom_top')")]
    Missing3dState,
    #[error("missing variable '{0}'")]
    MissingVariable(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Core(#[from] rustwx_core::RustwxError),
    #[error("unsupported wrf selector '{0}'")]
    UnsupportedSelector(FieldSelector),
}

#[derive(Debug, Clone)]
pub struct WrfSurfaceFields {
    pub lat: Vec<f64>,
    pub lon: Vec<f64>,
    pub nx: usize,
    pub ny: usize,
    pub psfc_pa: Vec<f64>,
    pub orog_m: Vec<f64>,
    pub t2_k: Vec<f64>,
    pub q2_kgkg: Vec<f64>,
    pub u10_ms: Vec<f64>,
    pub v10_ms: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct WrfPressureFields {
    pub nx: usize,
    pub ny: usize,
    pub pressure_levels_hpa: Vec<f64>,
    pub pressure_3d_pa: Vec<f64>,
    pub temperature_c_3d: Vec<f64>,
    pub qvapor_kgkg_3d: Vec<f64>,
    pub u_ms_3d: Vec<f64>,
    pub v_ms_3d: Vec<f64>,
    pub gh_m_3d: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct PartialSelection {
    pub extracted: Vec<SelectedField2D>,
    pub missing: Vec<FieldSelector>,
}

type SharedField = Arc<[f64]>;

pub fn looks_like_wrf(bytes: &[u8]) -> bool {
    bytes.starts_with(HDF5_SIGNATURE)
}

fn materialize_input(bytes: &[u8], preferred_path: Option<&Path>) -> Result<PathBuf, WrfError> {
    if !looks_like_wrf(bytes) {
        return Err(WrfError::NotWrfInput);
    }
    if let Some(path) = preferred_path {
        if path.exists() {
            return Ok(path.to_path_buf());
        }
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    let hash = hasher.finish();
    let path = std::env::temp_dir().join(format!("rustwx-wrf-{hash:016x}.nc"));
    if !path.exists() {
        fs::write(&path, bytes)?;
    }
    Ok(path)
}

pub struct WrfFile {
    nc: netcdf::File,
    pub nx: usize,
    pub ny: usize,
    pub nz: usize,
    nx_stag: usize,
    ny_stag: usize,
    nz_stag: usize,
    dx: f64,
    dy: f64,
    cache: Mutex<HashMap<String, SharedField>>,
}

impl WrfFile {
    pub fn open(path: &Path) -> Result<Self, WrfError> {
        let nc = netcdf::open(path).map_err(|err| WrfError::Netcdf(err.to_string()))?;
        let nx = dim_len(&nc, "west_east")?;
        let ny = dim_len(&nc, "south_north")?;
        let nz = nc.dimension("bottom_top").map(|d| d.len()).unwrap_or(0);
        let nx_stag = nc
            .dimension("west_east_stag")
            .map(|d| d.len())
            .unwrap_or(nx + 1);
        let ny_stag = nc
            .dimension("south_north_stag")
            .map(|d| d.len())
            .unwrap_or(ny + 1);
        let nz_stag = if nz > 0 {
            nc.dimension("bottom_top_stag")
                .map(|d| d.len())
                .unwrap_or(nz + 1)
        } else {
            0
        };
        let dx = global_attr_f64(&nc, "DX").unwrap_or(1000.0);
        let dy = global_attr_f64(&nc, "DY").unwrap_or(1000.0);
        Ok(Self {
            nc,
            nx,
            ny,
            nz,
            nx_stag,
            ny_stag,
            nz_stag,
            dx,
            dy,
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn nxy(&self) -> usize {
        self.nx * self.ny
    }

    pub fn nxyz(&self) -> usize {
        self.nz * self.nxy()
    }

    fn require_3d(&self) -> Result<(), WrfError> {
        if self.nz == 0 {
            Err(WrfError::Missing3dState)
        } else {
            Ok(())
        }
    }

    fn cached_or_compute<F>(&self, key: &str, f: F) -> Result<SharedField, WrfError>
    where
        F: FnOnce() -> Result<Vec<f64>, WrfError>,
    {
        if let Some(value) = self
            .cache
            .lock()
            .map_err(|_| WrfError::Netcdf("wrf cache poisoned".into()))?
            .get(key)
        {
            return Ok(Arc::clone(value));
        }
        let value = Arc::<[f64]>::from(f()?);
        self.cache
            .lock()
            .map_err(|_| WrfError::Netcdf("wrf cache poisoned".into()))?
            .insert(key.to_string(), Arc::clone(&value));
        Ok(value)
    }

    fn has_var(&self, name: &str) -> bool {
        self.nc.variable(name).is_some()
    }

    pub fn read_var(&self, name: &str) -> Result<Vec<f64>, WrfError> {
        let var = self
            .nc
            .variable(name)
            .ok_or_else(|| WrfError::MissingVariable(name.to_string()))?;
        let arr: ndarray::ArrayD<f64> = var
            .get(..)
            .map_err(|err| WrfError::Netcdf(err.to_string()))?;
        if arr.ndim() >= 3 {
            Ok(arr.index_axis(Axis(0), 0).iter().copied().collect())
        } else {
            Ok(arr.iter().copied().collect())
        }
    }

    fn read_var_optional(&self, name: &str, len: usize) -> Vec<f64> {
        self.read_var(name).unwrap_or_else(|_| vec![0.0; len])
    }

    fn reconstructed_latlon(&self) -> Result<(Vec<f64>, Vec<f64>), WrfError> {
        reconstruct_lambert_latlon(&self.nc, self.nx, self.ny, self.dx, self.dy)
    }

    pub fn full_pressure(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("pressure", || {
            let p = self.read_var("P")?;
            if self.has_var("PB") {
                let pb = self.read_var("PB")?;
                Ok(p.iter().zip(pb.iter()).map(|(a, b)| a + b).collect())
            } else {
                Ok(p)
            }
        })
    }

    pub fn full_theta(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("theta", || {
            Ok(self
                .read_var("T")?
                .into_iter()
                .map(|value| value + 300.0)
                .collect())
        })
    }

    pub fn full_geopotential(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("geopotential", || {
            if self.has_var("PH") && self.has_var("PHB") {
                let ph = self.read_var("PH")?;
                let phb = self.read_var("PHB")?;
                let stag = ph
                    .iter()
                    .zip(phb.iter())
                    .map(|(a, b)| a + b)
                    .collect::<Vec<_>>();
                Ok(destagger_z(&stag, self.nz_stag, self.ny, self.nx))
            } else if self.has_var("Z") {
                let z = self.read_var("Z")?;
                let nxy = self.nxy();
                let geopotential = if z.len() == self.nz * nxy {
                    z
                } else {
                    destagger_z(&z, self.nz_stag, self.ny, self.nx)
                };
                Ok(geopotential.into_iter().map(|value| value * G).collect())
            } else {
                Err(WrfError::MissingVariable("PH".to_string()))
            }
        })
    }

    pub fn temperature_k(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("temperature_k", || {
            if self.has_var("TK") {
                self.read_var("TK")
            } else {
                let theta = self.full_theta()?;
                let pressure = self.full_pressure()?;
                Ok(theta
                    .iter()
                    .zip(pressure.iter())
                    .map(|(th, p)| th * (p / P0).powf(KAPPA))
                    .collect())
            }
        })
    }

    pub fn temperature_c(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("temperature_c", || {
            Ok(self
                .temperature_k()?
                .iter()
                .map(|value| value - 273.15)
                .collect())
        })
    }

    pub fn height_msl(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("height_msl", || {
            Ok(self
                .full_geopotential()?
                .iter()
                .map(|value| value / G)
                .collect())
        })
    }

    pub fn terrain(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("terrain", || Ok(self.read_var_optional("HGT", self.nxy())))
    }

    pub fn height_agl(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("height_agl", || {
            let height = self.height_msl()?;
            let terrain = self.terrain()?;
            let nxy = self.nxy();
            Ok(height
                .iter()
                .enumerate()
                .map(|(idx, value)| value - terrain[idx % nxy])
                .collect())
        })
    }

    pub fn qvapor(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("qvapor", || self.read_var("QVAPOR"))
    }

    pub fn psfc(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("psfc", || self.read_var("PSFC"))
    }

    pub fn t2(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("t2", || self.read_var("T2"))
    }

    pub fn q2(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("q2", || self.read_var("Q2"))
    }

    pub fn sinalpha(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("sinalpha", || {
            Ok(self.read_var_optional("SINALPHA", self.nxy()))
        })
    }

    pub fn cosalpha(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("cosalpha", || {
            let values = self
                .read_var("COSALPHA")
                .unwrap_or_else(|_| vec![1.0; self.nxy()]);
            Ok(values)
        })
    }

    pub fn lat(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("lat", || {
            self.read_var("XLAT")
                .or_else(|_| self.read_var("XLAT_M"))
                .or_else(|_| self.reconstructed_latlon().map(|(lat, _)| lat))
        })
    }

    pub fn lon(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("lon", || {
            self.read_var("XLONG")
                .or_else(|_| self.read_var("XLONG_M"))
                .or_else(|_| self.reconstructed_latlon().map(|(_, lon)| lon))
        })
    }

    pub fn u10_earth(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("u10_earth", || {
            let u10 = self.read_var("U10")?;
            let v10 = self.read_var("V10")?;
            let sina = self.sinalpha()?;
            let cosa = self.cosalpha()?;
            let (u, _) = rotate_to_earth(&u10, &v10, &sina, &cosa, self.nxy());
            Ok(u)
        })
    }

    pub fn v10_earth(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("v10_earth", || {
            let u10 = self.read_var("U10")?;
            let v10 = self.read_var("V10")?;
            let sina = self.sinalpha()?;
            let cosa = self.cosalpha()?;
            let (_, v) = rotate_to_earth(&u10, &v10, &sina, &cosa, self.nxy());
            Ok(v)
        })
    }

    pub fn u_destag_raw(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("u_destag_raw", || {
            let u = self.read_var("U")?;
            Ok(destagger_x(&u, self.nz, self.ny, self.nx_stag))
        })
    }

    pub fn v_destag_raw(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("v_destag_raw", || {
            let v = self.read_var("V")?;
            Ok(destagger_y(&v, self.nz, self.ny_stag, self.nx))
        })
    }

    pub fn u_earth_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("u_earth_3d", || {
            let u = self.u_destag_raw()?;
            let v = self.v_destag_raw()?;
            let sina = self.sinalpha()?;
            let cosa = self.cosalpha()?;
            let (ue, _) = rotate_to_earth(&u, &v, &sina, &cosa, self.nxy());
            Ok(ue)
        })
    }

    pub fn v_earth_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("v_earth_3d", || {
            let u = self.u_destag_raw()?;
            let v = self.v_destag_raw()?;
            let sina = self.sinalpha()?;
            let cosa = self.cosalpha()?;
            let (_, ve) = rotate_to_earth(&u, &v, &sina, &cosa, self.nxy());
            Ok(ve)
        })
    }

    pub fn w_destag(&self) -> Result<SharedField, WrfError> {
        self.require_3d()?;
        self.cached_or_compute("w_destag", || {
            let w = self.read_var("W")?;
            Ok(destagger_z(&w, self.nz_stag, self.ny, self.nx))
        })
    }

    pub fn pressure_hpa_cached(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("pressure_hpa", || {
            Ok(self
                .full_pressure()?
                .iter()
                .map(|value| value / 100.0)
                .collect())
        })
    }

    pub fn relative_humidity_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("relative_humidity_3d", || {
            let temperature_c = self.temperature_c()?;
            let pressure = self.full_pressure()?;
            let qvapor = self.qvapor()?;
            Ok(temperature_c
                .iter()
                .zip(pressure.iter())
                .zip(qvapor.iter())
                .map(|((temp_c, pressure_pa), q)| {
                    relative_humidity_from_mixing_ratio(*temp_c, *pressure_pa / 100.0, *q)
                })
                .collect())
        })
    }

    pub fn dewpoint_k_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("dewpoint_k_3d", || {
            let pressure = self.full_pressure()?;
            let qvapor = self.qvapor()?;
            Ok(pressure
                .iter()
                .zip(qvapor.iter())
                .map(|(pressure_pa, q)| {
                    dewpoint_from_mixing_ratio(*pressure_pa / 100.0, *q) + 273.15
                })
                .collect())
        })
    }

    pub fn absolute_vorticity_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("absolute_vorticity_3d", || {
            let u = self.u_destag_raw()?;
            let v = self.v_destag_raw()?;
            let lat = self.lat()?;
            let nxy = self.nxy();
            let mut out = vec![0.0; self.nxyz()];
            for (k, plane) in out.chunks_mut(nxy).enumerate() {
                let level_offset = k * nxy;
                let u_plane = &u[level_offset..level_offset + nxy];
                let v_plane = &v[level_offset..level_offset + nxy];
                for j in 0..self.ny {
                    for i in 0..self.nx {
                        let ij = j * self.nx + i;
                        let dvdx = diff_x(v_plane, self.nx, i, j, self.dx);
                        let dudy = diff_y(u_plane, self.nx, self.ny, i, j, self.dy);
                        let coriolis = 2.0 * OMEGA * lat[ij].to_radians().sin();
                        plane[ij] = (dvdx - dudy) + coriolis;
                    }
                }
            }
            Ok(out)
        })
    }

    pub fn slp_pa(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("slp_pa", || {
            if self.nz == 0 {
                if self.has_var("P")
                    && self.has_var("TK")
                    && self.has_var("QVAPOR")
                    && self.has_var("Z")
                {
                    let pressure = self.read_var("P")?;
                    let temperature = self.read_var("TK")?;
                    let qvapor = self.read_var("QVAPOR")?;
                    let height = self.read_var("Z")?;
                    return Ok(pressure
                        .iter()
                        .zip(temperature.iter())
                        .zip(qvapor.iter())
                        .zip(height.iter())
                        .map(|(((pressure_pa, temperature_k), qv), z_m)| {
                            let tv = temperature_k * (1.0 + 0.608 * qv.max(0.0));
                            pressure_pa * (G_SLP * z_m.max(0.0) / (RD_SLP * tv.max(150.0))).exp()
                        })
                        .collect());
                }
                return self.psfc().map(|field| field.to_vec());
            }
            let pressure = self.full_pressure()?;
            let temperature = self.temperature_k()?;
            let qvapor = self.qvapor()?;
            let height = self.height_msl()?;
            let nxy = self.nxy();
            let mut slp = vec![0.0; nxy];
            for (ij, value) in slp.iter_mut().enumerate() {
                let p_sfc = pressure[ij];
                let mut klo = self.nz.saturating_sub(1);
                let mut found = false;
                for k in 0..self.nz {
                    if (p_sfc - pressure[k * nxy + ij]) >= PCONST {
                        klo = k;
                        found = true;
                        break;
                    }
                }
                if !found {
                    klo = self.nz.saturating_sub(1);
                }
                let khi = if klo > 0 {
                    klo - 1
                } else {
                    (klo + 1).min(self.nz.saturating_sub(1))
                };
                let plo = pressure[klo * nxy + ij];
                let phi = pressure[khi * nxy + ij];
                let qlo = qvapor[klo * nxy + ij].max(0.0);
                let qhi = qvapor[khi * nxy + ij].max(0.0);
                let tlo = temperature[klo * nxy + ij] * (1.0 + 0.608 * qlo);
                let thi = temperature[khi * nxy + ij] * (1.0 + 0.608 * qhi);
                let zlo = height[klo * nxy + ij];
                let zhi = height[khi * nxy + ij];
                let p_ref = p_sfc - PCONST;
                let (t_ref, z_ref) = if (plo - phi).abs() < 1.0 {
                    (tlo, zlo)
                } else {
                    let frac = (p_ref.ln() - phi.ln()) / (plo.ln() - phi.ln());
                    (thi + frac * (tlo - thi), zhi + frac * (zlo - zhi))
                };
                let t_surf = t_ref * (p_sfc / p_ref).powf(USSALR * RD_SLP / G_SLP);
                let mut t_sl = t_ref + USSALR * z_ref;
                if t_surf <= TC_SLP && t_sl >= TC_SLP {
                    t_sl = TC_SLP;
                } else {
                    t_sl = TC_SLP - 0.005 * (t_surf - TC_SLP) * (t_surf - TC_SLP);
                }
                let z_sfc = height[ij];
                *value = p_sfc * (2.0 * G_SLP * z_sfc / (RD_SLP * (t_sl + t_surf))).exp();
            }
            Ok(slp)
        })
    }

    pub fn pwat_kgm2(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("pwat_kgm2", || {
            if self.has_var("PWAT") {
                return Ok(self
                    .read_var("PWAT")?
                    .into_iter()
                    .map(|value| value * 1000.0)
                    .collect());
            }
            let qv = self.qvapor()?;
            let pressure = self.full_pressure()?;
            let mut out = vec![0.0; self.nxy()];
            for ij in 0..self.nxy() {
                let mut total = 0.0;
                for k in 0..self.nz.saturating_sub(1) {
                    let idx0 = k * self.nxy() + ij;
                    let idx1 = (k + 1) * self.nxy() + ij;
                    let dp = (pressure[idx0] - pressure[idx1]).abs();
                    let q_avg = 0.5 * (qv[idx0].max(0.0) + qv[idx1].max(0.0));
                    total += q_avg * dp;
                }
                out[ij] = total / G;
            }
            Ok(out)
        })
    }

    fn compute_cloud_cover_layer(
        &self,
        top_hpa: f64,
        bottom_hpa: f64,
    ) -> Result<Vec<f64>, WrfError> {
        let pressure = self.full_pressure()?;
        let qc = self.read_var_optional("QCLOUD", self.nxyz());
        let qi = self.read_var_optional("QICE", self.nxyz());
        let mut out = vec![0.0; self.nxy()];
        for ij in 0..self.nxy() {
            let mut cloudy = false;
            for k in 0..self.nz {
                let idx = k * self.nxy() + ij;
                let p_hpa = pressure[idx] / 100.0;
                if p_hpa <= bottom_hpa
                    && p_hpa >= top_hpa
                    && (qc[idx].max(0.0) + qi[idx].max(0.0)) > 1.0e-6
                {
                    cloudy = true;
                    break;
                }
            }
            out[ij] = if cloudy { 100.0 } else { 0.0 };
        }
        Ok(out)
    }

    pub fn cloud_cover_layers(&self) -> Result<(SharedField, SharedField, SharedField), WrfError> {
        let low = self.cached_or_compute("cloud_low", || {
            if self.has_var("CLDFRA") {
                self.compute_cloud_fraction_layer(800.0, 1100.0)
            } else {
                self.compute_cloud_cover_layer(800.0, 1100.0)
            }
        })?;
        let mid = self.cached_or_compute("cloud_mid", || {
            if self.has_var("CLDFRA") {
                self.compute_cloud_fraction_layer(450.0, 800.0)
            } else {
                self.compute_cloud_cover_layer(450.0, 800.0)
            }
        })?;
        let high = self.cached_or_compute("cloud_high", || {
            if self.has_var("CLDFRA") {
                self.compute_cloud_fraction_layer(0.0, 450.0)
            } else {
                self.compute_cloud_cover_layer(0.0, 450.0)
            }
        })?;
        Ok((low, mid, high))
    }

    fn compute_cloud_fraction_layer(
        &self,
        top_hpa: f64,
        bottom_hpa: f64,
    ) -> Result<Vec<f64>, WrfError> {
        let pressure = self.full_pressure()?;
        let cldfra = self.read_var("CLDFRA")?;
        let mut out = vec![0.0; self.nxy()];
        for ij in 0..self.nxy() {
            let mut max_fraction = 0.0_f64;
            for k in 0..self.nz {
                let idx = k * self.nxy() + ij;
                let p_hpa = pressure[idx] / 100.0;
                if p_hpa <= bottom_hpa && p_hpa >= top_hpa {
                    let fraction = cldfra[idx].clamp(0.0, 1.0);
                    if fraction > max_fraction {
                        max_fraction = fraction;
                    }
                }
            }
            out[ij] = max_fraction * 100.0;
        }
        Ok(out)
    }

    pub fn total_precip_mm(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("total_precip_mm", || {
            if self.has_var("PREC_ACC_C") || self.has_var("PREC_ACC_NC") {
                let conv = self.read_var_optional("PREC_ACC_C", self.nxy());
                let nonconv = self.read_var_optional("PREC_ACC_NC", self.nxy());
                return Ok(conv
                    .iter()
                    .zip(nonconv.iter())
                    .map(|(a, b)| a + b)
                    .collect());
            }
            if self.has_var("RAINC") || self.has_var("RAINNC") {
                let conv = self.read_var_optional("RAINC", self.nxy());
                let nonconv = self.read_var_optional("RAINNC", self.nxy());
                return Ok(conv
                    .iter()
                    .zip(nonconv.iter())
                    .map(|(a, b)| a + b)
                    .collect());
            }
            if self.has_var("ACRAINLSM") {
                return self.read_var("ACRAINLSM");
            }
            Err(WrfError::MissingVariable("PREC_ACC_NC".to_string()))
        })
    }

    pub fn dbz_3d(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("dbz_3d", || {
            if let Ok(refl) = self.read_var("REFL_10CM") {
                return Ok(refl);
            }
            let temperature = self.temperature_k()?;
            let pressure = self.full_pressure()?;
            let qvapor = self.qvapor()?;
            let qrain = self.read_var_optional("QRAIN", self.nxyz());
            let qsnow = self.read_var_optional("QSNOW", self.nxyz());
            let qgraup = self.read_var_optional("QGRAUP", self.nxyz());
            let factor_r = GAMMA_SEVEN * 1.0e18 * (1.0 / (PI * RHO_R)).powf(1.75);
            let factor_s = GAMMA_SEVEN
                * 1.0e18
                * (1.0 / (PI * RHO_S)).powf(1.75)
                * (RHO_S / RHOWAT).powi(2)
                * ALPHA;
            let factor_g = GAMMA_SEVEN
                * 1.0e18
                * (1.0 / (PI * RHO_G)).powf(1.75)
                * (RHO_G / RHOWAT).powi(2)
                * ALPHA;
            let out = (0..self.nxyz())
                .into_par_iter()
                .map(|idx| {
                    let t_k = temperature[idx];
                    let qv = qvapor[idx].max(0.0);
                    let virtual_t = t_k * (0.622 + qv) / (0.622 * (1.0 + qv));
                    let rhoair = pressure[idx] / (RD * virtual_t);
                    let mut qr = qrain[idx].max(0.0);
                    let mut qs = qsnow[idx].max(0.0);
                    let qg = qgraup[idx].max(0.0);
                    if qs == 0.0 && t_k < CELKEL {
                        qs = qr;
                        qr = 0.0;
                    }
                    let z_r = factor_r * (rhoair * qr).powf(1.75) / RN0_R.powf(0.75);
                    let z_s = factor_s * (rhoair * qs).powf(1.75) / RN0_S.powf(0.75);
                    let z_g = factor_g * (rhoair * qg).powf(1.75) / RN0_G.powf(0.75);
                    let z_e = (z_r + z_s + z_g).max(0.001);
                    10.0 * z_e.log10()
                })
                .collect();
            Ok(out)
        })
    }

    pub fn composite_reflectivity(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("composite_reflectivity", || {
            let dbz = self.dbz_3d()?;
            let mut out = vec![-999.0_f64; self.nxy()];
            for k in 0..self.nz {
                let offset = k * self.nxy();
                for ij in 0..self.nxy() {
                    out[ij] = out[ij].max(dbz[offset + ij]);
                }
            }
            Ok(out)
        })
    }

    pub fn updraft_helicity_2to5km(&self) -> Result<SharedField, WrfError> {
        self.cached_or_compute("uh_2to5km", || {
            let w = self.w_destag()?;
            let u = self.u_destag_raw()?;
            let v = self.v_destag_raw()?;
            let h_agl = self.height_agl()?;
            let mapfac = self.read_var_optional("MAPFAC_M", self.nxy());
            let twodx = 2.0 * self.dx;
            let twody = 2.0 * self.dy;
            let mut vort_3d = vec![0.0; self.nxyz()];
            for (k, plane) in vort_3d.chunks_mut(self.nxy()).enumerate() {
                let level_offset = k * self.nxy();
                let u_plane = &u[level_offset..level_offset + self.nxy()];
                let v_plane = &v[level_offset..level_offset + self.nxy()];
                for j in 0..self.ny {
                    for i in 0..self.nx {
                        let ij = j * self.nx + i;
                        let m = mapfac[ij].max(1.0e-6);
                        let dvdx = if self.nx < 2 {
                            0.0
                        } else if i == 0 {
                            (v_plane[j * self.nx + 1] - v_plane[j * self.nx]) / (self.dx * m)
                        } else if i == self.nx - 1 {
                            (v_plane[j * self.nx + i] - v_plane[j * self.nx + i - 1])
                                / (self.dx * m)
                        } else {
                            (v_plane[j * self.nx + i + 1] - v_plane[j * self.nx + i - 1])
                                / (twodx * m)
                        };
                        let dudy = if self.ny < 2 {
                            0.0
                        } else if j == 0 {
                            (u_plane[self.nx + i] - u_plane[i]) / (self.dy * m)
                        } else if j == self.ny - 1 {
                            (u_plane[j * self.nx + i] - u_plane[(j - 1) * self.nx + i])
                                / (self.dy * m)
                        } else {
                            (u_plane[(j + 1) * self.nx + i] - u_plane[(j - 1) * self.nx + i])
                                / (twody * m)
                        };
                        plane[ij] = dvdx - dudy;
                    }
                }
            }
            let mut out = vec![0.0; self.nxy()];
            for (ij, value) in out.iter_mut().enumerate() {
                let mut w_sum = 0.0;
                let mut depth = 0.0;
                for k in 0..self.nz.saturating_sub(1) {
                    let idx0 = k * self.nxy() + ij;
                    let idx1 = (k + 1) * self.nxy() + ij;
                    let h0 = h_agl[idx0];
                    let h1 = h_agl[idx1];
                    if h1 <= 2000.0 || h0 >= 5000.0 {
                        continue;
                    }
                    let z_lo = h0.max(2000.0);
                    let z_hi = h1.min(5000.0);
                    let dz = z_hi - z_lo;
                    if dz <= 0.0 {
                        continue;
                    }
                    let w_lo = if z_lo > h0 {
                        lerp_at(z_lo, h0, h1, w[idx0], w[idx1])
                    } else {
                        w[idx0]
                    };
                    let w_hi = if z_hi < h1 {
                        lerp_at(z_hi, h0, h1, w[idx0], w[idx1])
                    } else {
                        w[idx1]
                    };
                    w_sum += 0.5 * (w_lo + w_hi) * dz;
                    depth += dz;
                }
                if depth <= 0.0 || (w_sum / depth) <= 0.0 {
                    continue;
                }
                let mut integral = 0.0;
                for k in 0..self.nz.saturating_sub(1) {
                    let idx0 = k * self.nxy() + ij;
                    let idx1 = (k + 1) * self.nxy() + ij;
                    let h0 = h_agl[idx0];
                    let h1 = h_agl[idx1];
                    if h1 <= 2000.0 || h0 >= 5000.0 {
                        continue;
                    }
                    let z_lo = h0.max(2000.0);
                    let z_hi = h1.min(5000.0);
                    let dz = z_hi - z_lo;
                    if dz <= 0.0 {
                        continue;
                    }
                    let t0 = w[idx0] * vort_3d[idx0];
                    let t1 = w[idx1] * vort_3d[idx1];
                    let tem_lo = if z_lo > h0 {
                        lerp_at(z_lo, h0, h1, t0, t1)
                    } else {
                        t0
                    };
                    let tem_hi = if z_hi < h1 {
                        lerp_at(z_hi, h0, h1, t0, t1)
                    } else {
                        t1
                    };
                    integral += 0.5 * (tem_lo + tem_hi) * dz;
                }
                *value = integral;
            }
            Ok(out)
        })
    }
}

pub fn decode_surface_from_bytes(
    bytes: &[u8],
    preferred_path: Option<&Path>,
) -> Result<WrfSurfaceFields, WrfError> {
    let path = materialize_input(bytes, preferred_path)?;
    decode_surface_from_path(&path)
}

pub fn decode_surface_from_path(path: &Path) -> Result<WrfSurfaceFields, WrfError> {
    let file = WrfFile::open(path)?;
    Ok(WrfSurfaceFields {
        lat: file.lat()?.to_vec(),
        lon: file.lon()?.to_vec(),
        nx: file.nx,
        ny: file.ny,
        psfc_pa: file.psfc()?.to_vec(),
        orog_m: file.terrain()?.to_vec(),
        t2_k: file.t2()?.to_vec(),
        q2_kgkg: file.q2()?.to_vec(),
        u10_ms: file.u10_earth()?.to_vec(),
        v10_ms: file.v10_earth()?.to_vec(),
    })
}

pub fn decode_pressure_from_bytes(
    bytes: &[u8],
    preferred_path: Option<&Path>,
) -> Result<WrfPressureFields, WrfError> {
    let path = materialize_input(bytes, preferred_path)?;
    decode_pressure_from_path(&path)
}

pub fn decode_pressure_from_path(path: &Path) -> Result<WrfPressureFields, WrfError> {
    let file = WrfFile::open(path)?;
    let pressure_3d_pa = file.full_pressure()?.to_vec();
    Ok(WrfPressureFields {
        nx: file.nx,
        ny: file.ny,
        pressure_levels_hpa: mean_pressure_levels_hpa(&pressure_3d_pa, file.nx, file.ny, file.nz),
        pressure_3d_pa,
        temperature_c_3d: file.temperature_c()?.to_vec(),
        qvapor_kgkg_3d: file.qvapor()?.to_vec(),
        u_ms_3d: file.u_earth_3d()?.to_vec(),
        v_ms_3d: file.v_earth_3d()?.to_vec(),
        gh_m_3d: file.height_msl()?.to_vec(),
    })
}

pub fn extract_selectors_partial_from_bytes(
    bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
) -> Result<PartialSelection, WrfError> {
    let path = materialize_input(bytes, preferred_path)?;
    extract_selectors_partial_from_path(&path, selectors)
}

pub fn extract_selectors_partial_from_path(
    path: &Path,
    selectors: &[FieldSelector],
) -> Result<PartialSelection, WrfError> {
    let file = WrfFile::open(path)?;
    let grid = latlon_grid(&file)?;
    let mut extracted = Vec::new();
    let mut missing = Vec::new();
    for selector in selectors {
        match extract_selector(&file, &grid, *selector) {
            Ok(field) => extracted.push(field),
            Err(
                WrfError::UnsupportedSelector(_)
                | WrfError::Missing3dState
                | WrfError::MissingVariable(_),
            ) => missing.push(*selector),
            Err(err) => return Err(err),
        }
    }
    Ok(PartialSelection { extracted, missing })
}

fn extract_selector(
    file: &WrfFile,
    grid: &LatLonGrid,
    selector: FieldSelector,
) -> Result<SelectedField2D, WrfError> {
    let values = match selector {
        FieldSelector {
            field: CanonicalField::Temperature,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => file.t2()?.to_vec(),
        FieldSelector {
            field: CanonicalField::Dewpoint,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => file
            .q2()?
            .iter()
            .zip(file.psfc()?.iter())
            .map(|(q, p_pa)| dewpoint_from_mixing_ratio(*p_pa / 100.0, *q) + 273.15)
            .collect(),
        FieldSelector {
            field: CanonicalField::RelativeHumidity,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => file
            .t2()?
            .iter()
            .zip(file.q2()?.iter())
            .zip(file.psfc()?.iter())
            .map(|((t_k, q), p_pa)| {
                relative_humidity_from_mixing_ratio(*t_k - 273.15, *p_pa / 100.0, *q)
            })
            .collect(),
        FieldSelector {
            field: CanonicalField::UWind,
            vertical: VerticalSelector::HeightAboveGroundMeters(10),
        } => file.u10_earth()?.to_vec(),
        FieldSelector {
            field: CanonicalField::VWind,
            vertical: VerticalSelector::HeightAboveGroundMeters(10),
        } => file.v10_earth()?.to_vec(),
        FieldSelector {
            field: CanonicalField::PressureReducedToMeanSeaLevel,
            vertical: VerticalSelector::MeanSeaLevel,
        } => file.slp_pa()?.to_vec(),
        FieldSelector {
            field: CanonicalField::PrecipitableWater,
            vertical: VerticalSelector::EntireAtmosphere,
        } => file.pwat_kgm2()?.to_vec(),
        FieldSelector {
            field: CanonicalField::TotalPrecipitation,
            vertical: VerticalSelector::Surface,
        } => file.total_precip_mm()?.to_vec(),
        FieldSelector {
            field: CanonicalField::LowCloudCover,
            vertical: VerticalSelector::EntireAtmosphere,
        } => file.cloud_cover_layers()?.0.to_vec(),
        FieldSelector {
            field: CanonicalField::MiddleCloudCover,
            vertical: VerticalSelector::EntireAtmosphere,
        } => file.cloud_cover_layers()?.1.to_vec(),
        FieldSelector {
            field: CanonicalField::HighCloudCover,
            vertical: VerticalSelector::EntireAtmosphere,
        } => file.cloud_cover_layers()?.2.to_vec(),
        FieldSelector {
            field: CanonicalField::TotalCloudCover,
            vertical: VerticalSelector::EntireAtmosphere,
        } => {
            let (low, mid, high) = file.cloud_cover_layers()?;
            low.iter()
                .zip(mid.iter())
                .zip(high.iter())
                .map(|((l, m), h)| l.max(*m).max(*h))
                .collect()
        }
        FieldSelector {
            field: CanonicalField::CompositeReflectivity,
            vertical: VerticalSelector::EntireAtmosphere,
        } => file.composite_reflectivity()?.to_vec(),
        FieldSelector {
            field: CanonicalField::RadarReflectivity,
            vertical: VerticalSelector::HeightAboveGroundMeters(1000),
        } => interp_to_height_level(
            &file.dbz_3d()?,
            &file.height_agl()?,
            file.nx,
            file.ny,
            file.nz,
            1000.0,
        ),
        FieldSelector {
            field: CanonicalField::UpdraftHelicity,
            vertical:
                VerticalSelector::HeightAboveGroundLayerMeters {
                    bottom_m: 2000,
                    top_m: 5000,
                },
        } => file.updraft_helicity_2to5km()?.to_vec(),
        FieldSelector {
            field,
            vertical: VerticalSelector::IsobaricHpa(level_hpa),
        } if matches!(
            field,
            CanonicalField::Temperature
                | CanonicalField::GeopotentialHeight
                | CanonicalField::RelativeHumidity
                | CanonicalField::Dewpoint
                | CanonicalField::UWind
                | CanonicalField::VWind
                | CanonicalField::AbsoluteVorticity
        ) =>
        {
            let pressure_hpa = file.pressure_hpa_cached()?;
            match field {
                CanonicalField::Temperature => interp_to_pressure_level(
                    &file.temperature_k()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::GeopotentialHeight => interp_to_pressure_level(
                    &file.height_msl()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::RelativeHumidity => interp_to_pressure_level(
                    &file.relative_humidity_3d()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::Dewpoint => interp_to_pressure_level(
                    &file.dewpoint_k_3d()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::UWind => interp_to_pressure_level(
                    &file.u_earth_3d()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::VWind => interp_to_pressure_level(
                    &file.v_earth_3d()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                CanonicalField::AbsoluteVorticity => interp_to_pressure_level(
                    &file.absolute_vorticity_3d()?,
                    &pressure_hpa,
                    file.nx,
                    file.ny,
                    file.nz,
                    level_hpa as f64,
                ),
                _ => unreachable!(),
            }
        }
        _ => return Err(WrfError::UnsupportedSelector(selector)),
    };

    SelectedField2D::new(
        selector,
        selector.native_units(),
        grid.clone(),
        values.into_iter().map(|value| value as f32).collect(),
    )
    .map_err(Into::into)
}

fn dim_len(file: &netcdf::File, name: &str) -> Result<usize, WrfError> {
    file.dimension(name)
        .map(|dimension| dimension.len())
        .ok_or_else(|| WrfError::MissingDimension(name.to_string()))
}

fn global_attr_f64(file: &netcdf::File, name: &str) -> Result<f64, WrfError> {
    let attr = file
        .attribute(name)
        .ok_or_else(|| WrfError::MissingVariable(name.to_string()))?;
    let value = attr
        .value()
        .map_err(|err| WrfError::Netcdf(err.to_string()))?;
    match value {
        netcdf::AttributeValue::Double(value) => Ok(value),
        netcdf::AttributeValue::Float(value) => Ok(value as f64),
        netcdf::AttributeValue::Int(value) => Ok(value as f64),
        _ => Err(WrfError::Netcdf(format!(
            "attribute '{name}' was not numeric"
        ))),
    }
}

fn reconstruct_lambert_latlon(
    file: &netcdf::File,
    nx: usize,
    ny: usize,
    dx: f64,
    dy: f64,
) -> Result<(Vec<f64>, Vec<f64>), WrfError> {
    let map_proj = global_attr_f64(file, "MAP_PROJ").unwrap_or(0.0).round() as i32;
    if map_proj != 1 {
        return Err(WrfError::MissingVariable("XLAT".to_string()));
    }

    let cen_lat = global_attr_f64(file, "CEN_LAT")?.to_radians();
    let cen_lon = global_attr_f64(file, "CEN_LON")?.to_radians();
    let truelat1 = global_attr_f64(file, "TRUELAT1")?.to_radians();
    let truelat2 = global_attr_f64(file, "TRUELAT2")?.to_radians();
    let stand_lon = global_attr_f64(file, "STAND_LON")?.to_radians();

    let n = if (truelat1 - truelat2).abs() < 1.0e-10 {
        truelat1.sin()
    } else {
        (truelat1.cos() / truelat2.cos()).ln()
            / ((PI * 0.25 + truelat2 * 0.5).tan() / (PI * 0.25 + truelat1 * 0.5).tan()).ln()
    };
    let f = truelat1.cos() * (PI * 0.25 + truelat1 * 0.5).tan().powf(n) / n;
    let rho0 = EARTH_RADIUS_M * f / (PI * 0.25 + cen_lat * 0.5).tan().powf(n);

    let x_center = (nx as f64 - 1.0) * 0.5;
    let y_center = (ny as f64 - 1.0) * 0.5;
    let mut lat = vec![0.0; nx * ny];
    let mut lon = vec![0.0; nx * ny];

    for j in 0..ny {
        let y = (j as f64 - y_center) * dy;
        for i in 0..nx {
            let x = (i as f64 - x_center) * dx;
            let rho = (x * x + (rho0 - y) * (rho0 - y)).sqrt().max(1.0e-12);
            let theta = x.atan2(rho0 - y);
            let phi = 2.0 * (EARTH_RADIUS_M * f / rho).powf(1.0 / n).atan() - PI * 0.5;
            let lambda = stand_lon + theta / n;
            let idx = j * nx + i;
            lat[idx] = phi.to_degrees();
            lon[idx] = lambda.to_degrees();
        }
    }

    let lon_center = cen_lon.to_degrees();
    for value in &mut lon {
        while *value - lon_center > 180.0 {
            *value -= 360.0;
        }
        while *value - lon_center < -180.0 {
            *value += 360.0;
        }
    }

    Ok((lat, lon))
}

fn latlon_grid(file: &WrfFile) -> Result<LatLonGrid, WrfError> {
    let shape = GridShape::new(file.nx, file.ny)?;
    let lat = file.lat()?.iter().map(|value| *value as f32).collect();
    let lon = file.lon()?.iter().map(|value| *value as f32).collect();
    LatLonGrid::new(shape, lat, lon).map_err(Into::into)
}

fn mean_pressure_levels_hpa(pressure_3d_pa: &[f64], nx: usize, ny: usize, nz: usize) -> Vec<f64> {
    let nxy = nx * ny;
    (0..nz)
        .map(|k| {
            let slice = &pressure_3d_pa[k * nxy..(k + 1) * nxy];
            let sum = slice.iter().sum::<f64>();
            (sum / slice.len() as f64) / 100.0
        })
        .collect()
}

fn rotate_to_earth(
    u: &[f64],
    v: &[f64],
    sina: &[f64],
    cosa: &[f64],
    nxy: usize,
) -> (Vec<f64>, Vec<f64>) {
    let mut u_earth = vec![0.0; u.len()];
    let mut v_earth = vec![0.0; v.len()];
    for idx in 0..u.len() {
        let ij = idx % nxy;
        let ca = cosa[ij];
        let sa = sina[ij];
        u_earth[idx] = u[idx] * ca - v[idx] * sa;
        v_earth[idx] = u[idx] * sa + v[idx] * ca;
    }
    (u_earth, v_earth)
}

fn destagger_x(values: &[f64], nz: usize, ny: usize, nx_stag: usize) -> Vec<f64> {
    let actual_nx_stag = if nz == 0 || ny == 0 {
        0
    } else {
        values.len() / (nz * ny)
    };
    let nx = nx_stag.min(actual_nx_stag).saturating_sub(1);
    let mut out = vec![0.0; nz * ny * nx];
    for k in 0..nz {
        for j in 0..ny {
            for i in 0..nx {
                let left = k * ny * actual_nx_stag + j * actual_nx_stag + i;
                let right = left + 1;
                out[k * ny * nx + j * nx + i] = 0.5 * (values[left] + values[right]);
            }
        }
    }
    out
}

fn destagger_y(values: &[f64], nz: usize, ny_stag: usize, nx: usize) -> Vec<f64> {
    let actual_ny_stag = if nz == 0 || nx == 0 {
        0
    } else {
        values.len() / (nz * nx)
    };
    let ny = ny_stag.min(actual_ny_stag).saturating_sub(1);
    let mut out = vec![0.0; nz * ny * nx];
    for k in 0..nz {
        for j in 0..ny {
            for i in 0..nx {
                let south = k * actual_ny_stag * nx + j * nx + i;
                let north = south + nx;
                out[k * ny * nx + j * nx + i] = 0.5 * (values[south] + values[north]);
            }
        }
    }
    out
}

fn destagger_z(values: &[f64], nz_stag: usize, ny: usize, nx: usize) -> Vec<f64> {
    let nxy = ny * nx;
    let actual_nz_stag = if nxy == 0 { 0 } else { values.len() / nxy };
    let nz = nz_stag.min(actual_nz_stag).saturating_sub(1);
    let mut out = vec![0.0; nz * nxy];
    for k in 0..nz {
        let lower = k * nxy;
        let upper = (k + 1) * nxy;
        for ij in 0..nxy {
            out[k * nxy + ij] = 0.5 * (values[lower + ij] + values[upper + ij]);
        }
    }
    out
}

fn diff_x(values: &[f64], nx: usize, i: usize, j: usize, dx: f64) -> f64 {
    if nx < 2 {
        0.0
    } else if i == 0 {
        (values[j * nx + 1] - values[j * nx]) / dx
    } else if i == nx - 1 {
        (values[j * nx + i] - values[j * nx + i - 1]) / dx
    } else {
        (values[j * nx + i + 1] - values[j * nx + i - 1]) / (2.0 * dx)
    }
}

fn diff_y(values: &[f64], nx: usize, ny: usize, i: usize, j: usize, dy: f64) -> f64 {
    if ny < 2 {
        0.0
    } else if j == 0 {
        (values[nx + i] - values[i]) / dy
    } else if j == ny - 1 {
        (values[j * nx + i] - values[(j - 1) * nx + i]) / dy
    } else {
        (values[(j + 1) * nx + i] - values[(j - 1) * nx + i]) / (2.0 * dy)
    }
}

fn relative_humidity_from_mixing_ratio(
    temp_c: f64,
    pressure_hpa: f64,
    mixing_ratio_kgkg: f64,
) -> f64 {
    let q = mixing_ratio_kgkg.max(1.0e-10);
    let vapor_pressure_hpa = q * pressure_hpa / (0.622 + q);
    let saturation_vapor_pressure_hpa = 6.112 * ((17.67 * temp_c) / (temp_c + 243.5)).exp();
    (vapor_pressure_hpa / saturation_vapor_pressure_hpa * 100.0).clamp(0.0, 100.0)
}

fn dewpoint_from_mixing_ratio(pressure_hpa: f64, mixing_ratio_kgkg: f64) -> f64 {
    let q = mixing_ratio_kgkg.max(1.0e-10);
    let vapor_pressure_hpa = (q * pressure_hpa / (0.622 + q)).max(1.0e-10);
    let ln_e = (vapor_pressure_hpa / 6.112).ln();
    (243.5 * ln_e) / (17.67 - ln_e)
}

fn interp_to_pressure_level(
    field_3d: &[f64],
    pressure_3d_hpa: &[f64],
    nx: usize,
    ny: usize,
    nz: usize,
    target_hpa: f64,
) -> Vec<f64> {
    let n2d = nx * ny;
    let log_target = target_hpa.ln();
    (0..n2d)
        .into_par_iter()
        .map(|ij| {
            let mut column = extract_column(pressure_3d_hpa, nz, n2d, ij);
            let mut values = extract_column(field_3d, nz, n2d, ij);
            if column.len() > 1 && column[0] < column[column.len() - 1] {
                column.reverse();
                values.reverse();
            }
            if column.is_empty()
                || target_hpa >= column[0]
                || target_hpa <= column[column.len() - 1]
            {
                return f64::NAN;
            }
            for k in 0..column.len().saturating_sub(1) {
                if column[k] >= target_hpa && target_hpa >= column[k + 1] {
                    let log_lo = column[k].ln();
                    let log_hi = column[k + 1].ln();
                    let denom = log_hi - log_lo;
                    if denom.abs() < 1.0e-12 {
                        return 0.5 * (values[k] + values[k + 1]);
                    }
                    let frac = (log_target - log_lo) / denom;
                    return values[k] + frac * (values[k + 1] - values[k]);
                }
            }
            f64::NAN
        })
        .collect()
}

fn interp_to_height_level(
    field_3d: &[f64],
    height_agl_3d: &[f64],
    nx: usize,
    ny: usize,
    nz: usize,
    target_m: f64,
) -> Vec<f64> {
    let n2d = nx * ny;
    (0..n2d)
        .into_par_iter()
        .map(|ij| {
            let mut heights = extract_column(height_agl_3d, nz, n2d, ij);
            let mut values = extract_column(field_3d, nz, n2d, ij);
            if heights.len() > 1 && heights[0] > heights[heights.len() - 1] {
                heights.reverse();
                values.reverse();
            }
            if heights.is_empty()
                || target_m <= heights[0]
                || target_m >= heights[heights.len() - 1]
            {
                return f64::NAN;
            }
            for k in 0..heights.len().saturating_sub(1) {
                if heights[k] <= target_m && target_m <= heights[k + 1] {
                    return lerp_at(
                        target_m,
                        heights[k],
                        heights[k + 1],
                        values[k],
                        values[k + 1],
                    );
                }
            }
            f64::NAN
        })
        .collect()
}

fn extract_column(values: &[f64], nz: usize, n2d: usize, ij: usize) -> Vec<f64> {
    (0..nz).map(|k| values[k * n2d + ij]).collect()
}

fn lerp_at(z: f64, z0: f64, z1: f64, v0: f64, v1: f64) -> f64 {
    if (z1 - z0).abs() < 1.0e-6 {
        0.5 * (v0 + v1)
    } else {
        let frac = (z - z0) / (z1 - z0);
        v0 + frac * (v1 - v0)
    }
}
