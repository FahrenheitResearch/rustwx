use rustwx_core::{
    CanonicalField, FieldSelector, GridShape, LatLonGrid, SelectedField2D, VerticalSelector,
};
use std::path::{Path, PathBuf};

const G: f64 = 9.80665;

#[derive(Debug, thiserror::Error)]
pub enum Earth2ArchiveError {
    #[error("Earth2Archive extraction needs a local NetCDF path")]
    MissingPath,
    #[error("local Earth2Archive path does not exist: {0}")]
    MissingFile(String),
    #[error("netcdf error: {0}")]
    Netcdf(String),
    #[error("missing variable '{0}'")]
    MissingVariable(String),
    #[error("unsupported Earth2Archive selector '{0}'")]
    UnsupportedSelector(FieldSelector),
    #[error(transparent)]
    Core(#[from] rustwx_core::RustwxError),
}

#[derive(Debug, Clone)]
pub struct PartialSelection {
    pub extracted: Vec<SelectedField2D>,
    pub missing: Vec<FieldSelector>,
}

pub fn archive_path_from_url(url: &str) -> Option<PathBuf> {
    let rest = url.strip_prefix("earth2-archive://")?;
    let root = std::env::var_os("RUSTWX_EARTH2_ARCHIVE").map(PathBuf::from)?;
    let mut out = root;
    for part in rest.split('/') {
        if !part.is_empty() {
            out.push(part);
        }
    }
    Some(out)
}

pub fn url_is_earth2_archive(url: &str) -> bool {
    url.starts_with("earth2-archive://")
}

pub fn extract_selectors_partial_from_path(
    path: &Path,
    selectors: &[FieldSelector],
) -> Result<PartialSelection, Earth2ArchiveError> {
    if !path.exists() {
        return Err(Earth2ArchiveError::MissingFile(path.display().to_string()));
    }
    let file = netcrust::open(path).map_err(|err| Earth2ArchiveError::Netcdf(err.to_string()))?;
    let grid = read_latlon_grid(&file)?;
    let mut extracted = Vec::new();
    let mut missing = Vec::new();
    for selector in selectors {
        match extract_selector(&file, &grid, *selector) {
            Ok(field) => extracted.push(field),
            Err(
                Earth2ArchiveError::UnsupportedSelector(_) | Earth2ArchiveError::MissingVariable(_),
            ) => missing.push(*selector),
            Err(err) => return Err(err),
        }
    }
    Ok(PartialSelection { extracted, missing })
}

pub fn extract_selectors_partial_from_bytes(
    _bytes: &[u8],
    preferred_path: Option<&Path>,
    selectors: &[FieldSelector],
) -> Result<PartialSelection, Earth2ArchiveError> {
    let path = preferred_path.ok_or(Earth2ArchiveError::MissingPath)?;
    extract_selectors_partial_from_path(path, selectors)
}

fn extract_selector(
    file: &netcrust::File,
    grid: &LatLonGrid,
    selector: FieldSelector,
) -> Result<SelectedField2D, Earth2ArchiveError> {
    let values = match selector {
        FieldSelector {
            field: CanonicalField::Temperature,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => read_2d(file, "t2m")?,
        FieldSelector {
            field: CanonicalField::Dewpoint,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => read_2d(file, "d2m")?,
        FieldSelector {
            field: CanonicalField::RelativeHumidity,
            vertical: VerticalSelector::HeightAboveGroundMeters(2),
        } => {
            let t = read_2d(file, "t2m")?;
            let d = read_2d(file, "d2m")?;
            t.iter()
                .zip(d.iter())
                .map(|(t_k, td_k)| relative_humidity_from_t_td(*t_k, *td_k))
                .collect()
        }
        FieldSelector {
            field: CanonicalField::UWind,
            vertical: VerticalSelector::HeightAboveGroundMeters(10),
        } => read_2d(file, "u10m")?,
        FieldSelector {
            field: CanonicalField::VWind,
            vertical: VerticalSelector::HeightAboveGroundMeters(10),
        } => read_2d(file, "v10m")?,
        FieldSelector {
            field: CanonicalField::Pressure,
            vertical: VerticalSelector::Surface,
        } => read_2d(file, "sp")?,
        FieldSelector {
            field: CanonicalField::PressureReducedToMeanSeaLevel,
            vertical: VerticalSelector::MeanSeaLevel,
        } => read_2d(file, "msl")?,
        FieldSelector {
            field: CanonicalField::PrecipitableWater,
            vertical: VerticalSelector::EntireAtmosphere,
        } => read_2d(file, "tcw")?,
        FieldSelector {
            field: CanonicalField::TotalCloudCover,
            vertical: VerticalSelector::EntireAtmosphere,
        } => read_2d(file, "tcc")?
            .into_iter()
            .map(|value| if value <= 1.5 { value * 100.0 } else { value })
            .collect(),
        FieldSelector {
            field: CanonicalField::TotalPrecipitation,
            vertical: VerticalSelector::Surface,
        } => read_2d(file, "tp06")?
            .into_iter()
            .map(|meters| meters * 1000.0)
            .collect(),
        FieldSelector {
            field,
            vertical: VerticalSelector::IsobaricHpa(level_hpa),
        } => read_isobaric_selector(file, field, level_hpa)?,
        _ => return Err(Earth2ArchiveError::UnsupportedSelector(selector)),
    };

    SelectedField2D::new(
        selector,
        selector.native_units(),
        grid.clone(),
        values.into_iter().map(|value| value as f32).collect(),
    )
    .map_err(Into::into)
}

fn read_isobaric_selector(
    file: &netcrust::File,
    field: CanonicalField,
    level_hpa: u16,
) -> Result<Vec<f64>, Earth2ArchiveError> {
    let name = match field {
        CanonicalField::Temperature => format!("t{level_hpa}"),
        CanonicalField::UWind => format!("u{level_hpa}"),
        CanonicalField::VWind => format!("v{level_hpa}"),
        CanonicalField::GeopotentialHeight => {
            return Ok(read_2d(file, &format!("z{level_hpa}"))?
                .into_iter()
                .map(|value| value / G)
                .collect());
        }
        CanonicalField::Dewpoint => {
            let q = read_2d(file, &format!("q{level_hpa}"))?;
            return Ok(q
                .into_iter()
                .map(|value| dewpoint_from_specific_humidity(level_hpa as f64, value))
                .collect());
        }
        CanonicalField::RelativeHumidity => {
            let t = read_2d(file, &format!("t{level_hpa}"))?;
            let q = read_2d(file, &format!("q{level_hpa}"))?;
            return Ok(t
                .into_iter()
                .zip(q)
                .map(|(t_k, q)| relative_humidity_from_q(level_hpa as f64, t_k, q))
                .collect());
        }
        _ => {
            return Err(Earth2ArchiveError::UnsupportedSelector(
                FieldSelector::isobaric(field, level_hpa),
            ));
        }
    };
    read_2d(file, &name)
}

fn read_latlon_grid(file: &netcrust::File) -> Result<LatLonGrid, Earth2ArchiveError> {
    let lat_1d = file
        .read_f64("lat")
        .map_err(|err| Earth2ArchiveError::Netcdf(err.to_string()))?;
    let lon_1d = file
        .read_f64("lon")
        .map_err(|err| Earth2ArchiveError::Netcdf(err.to_string()))?;
    let ny = lat_1d.len();
    let nx = lon_1d.len();
    if nx == 0 || ny == 0 {
        return Err(Earth2ArchiveError::Netcdf(
            "lat/lon dimensions must be non-empty".to_string(),
        ));
    }
    let mut lat = Vec::with_capacity(nx * ny);
    let mut lon = Vec::with_capacity(nx * ny);
    for lat_value in lat_1d {
        for lon_value in &lon_1d {
            lat.push(lat_value as f32);
            lon.push(normalize_lon(*lon_value) as f32);
        }
    }
    rotate_rows_to_west_east(&mut lat, &mut lon, None, nx, ny);
    LatLonGrid::new(GridShape::new(nx, ny)?, lat, lon).map_err(Into::into)
}

fn read_2d(file: &netcrust::File, name: &str) -> Result<Vec<f64>, Earth2ArchiveError> {
    let array = file
        .read_array_f64_first_record_or_all(name)
        .map_err(|err| match err {
            netcrust::Error::VariableNotFound(_) => {
                Earth2ArchiveError::MissingVariable(name.to_string())
            }
            _ => Earth2ArchiveError::Netcdf(err.to_string()),
        })?;
    let shape = array.shape().to_vec();
    if shape.len() != 2 {
        return Err(Earth2ArchiveError::Netcdf(format!(
            "variable '{name}' did not reduce to a 2-D field; shape={shape:?}"
        )));
    }
    let ny = shape[0];
    let nx = shape[1];
    let mut values = array.into_values();
    let mut dummy_lat = vec![0.0_f32; nx * ny];
    let mut dummy_lon = (0..ny)
        .flat_map(|_| (0..nx).map(|i| normalize_lon(i as f64 * 360.0 / nx as f64) as f32))
        .collect::<Vec<_>>();
    rotate_rows_to_west_east(&mut dummy_lat, &mut dummy_lon, Some(&mut values), nx, ny);
    Ok(values)
}

fn rotate_rows_to_west_east(
    lat: &mut [f32],
    lon: &mut [f32],
    mut values: Option<&mut [f64]>,
    nx: usize,
    ny: usize,
) {
    if nx == 0 || ny == 0 {
        return;
    }
    let split = (0..nx).find(|&i| lon[i] < 0.0).unwrap_or(0);
    if split == 0 {
        return;
    }
    for row in 0..ny {
        let start = row * nx;
        let end = start + nx;
        lat[start..end].rotate_left(split);
        lon[start..end].rotate_left(split);
        if let Some(ref mut values) = values {
            values[start..end].rotate_left(split);
        }
    }
}

fn normalize_lon(lon: f64) -> f64 {
    let mut out = lon;
    while out > 180.0 {
        out -= 360.0;
    }
    while out < -180.0 {
        out += 360.0;
    }
    out
}

fn relative_humidity_from_t_td(t_k: f64, td_k: f64) -> f64 {
    let es = saturation_vapor_pressure_hpa(t_k - 273.15);
    let e = saturation_vapor_pressure_hpa(td_k - 273.15);
    (e / es * 100.0).clamp(0.0, 100.0)
}

fn relative_humidity_from_q(pressure_hpa: f64, t_k: f64, q_kgkg: f64) -> f64 {
    let vapor_hpa = vapor_pressure_from_specific_humidity(pressure_hpa, q_kgkg);
    let es = saturation_vapor_pressure_hpa(t_k - 273.15);
    (vapor_hpa / es * 100.0).clamp(0.0, 100.0)
}

fn dewpoint_from_specific_humidity(pressure_hpa: f64, q_kgkg: f64) -> f64 {
    let vapor_hpa = vapor_pressure_from_specific_humidity(pressure_hpa, q_kgkg);
    let ln_e = (vapor_hpa.max(1.0e-10) / 6.112).ln();
    (243.5 * ln_e) / (17.67 - ln_e) + 273.15
}

fn vapor_pressure_from_specific_humidity(pressure_hpa: f64, q_kgkg: f64) -> f64 {
    let q = q_kgkg.clamp(1.0e-10, 0.2);
    q * pressure_hpa / (0.622 + (1.0 - 0.622) * q)
}

fn saturation_vapor_pressure_hpa(temp_c: f64) -> f64 {
    6.112 * ((17.67 * temp_c) / (temp_c + 243.5)).exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earth2_url_resolves_under_archive_root() {
        unsafe {
            std::env::set_var("RUSTWX_EARTH2_ARCHIVE", r"C:\archive");
        }
        let path = archive_path_from_url("earth2-archive://aifs/20260502T00Z/lead024.nc")
            .expect("url resolves");
        assert!(path.ends_with(r"aifs\20260502T00Z\lead024.nc"));
    }

    #[test]
    fn dewpoint_from_specific_humidity_is_physical() {
        let td = dewpoint_from_specific_humidity(850.0, 0.008);
        assert!(td > 260.0 && td < 295.0);
    }

    #[test]
    fn extracts_real_aifs_archive_when_configured() {
        let Some(path) = archive_path_from_url("earth2-archive://aifs/20160822T00Z/lead024.nc")
        else {
            return;
        };
        if !path.exists() {
            return;
        }
        let partial = extract_selectors_partial_from_path(
            &path,
            &[
                FieldSelector::isobaric(CanonicalField::Temperature, 500),
                FieldSelector::isobaric(CanonicalField::GeopotentialHeight, 500),
                FieldSelector::height_agl(CanonicalField::Temperature, 2),
            ],
        )
        .expect("configured AIFS NetCDF should extract");
        assert_eq!(partial.missing, Vec::new());
        assert_eq!(partial.extracted.len(), 3);
    }
}
