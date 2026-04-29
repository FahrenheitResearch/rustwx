use super::{VolumeResult, VolumeStoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GridSpec {
    RegularLatLon {
        nx: usize,
        ny: usize,
        west_lon_deg: f64,
        east_lon_deg: f64,
        south_lat_deg: f64,
        north_lat_deg: f64,
    },
    LambertConformal {
        nx: usize,
        ny: usize,
        dx_m: f64,
        dy_m: f64,
        latin1_deg: f64,
        latin2_deg: f64,
        lov_deg: f64,
        la1_deg: f64,
        lo1_deg: f64,
        description: String,
    },
    CurvilinearLatLon {
        nx: usize,
        ny: usize,
        lat_deg: Vec<f32>,
        lon_deg: Vec<f32>,
        description: String,
    },
}

impl GridSpec {
    pub fn validate(&self) -> VolumeResult<()> {
        if self.nx() == 0 || self.ny() == 0 {
            return Err(VolumeStoreError::InvalidManifest(
                "grid nx/ny must be positive".to_string(),
            ));
        }
        if let Self::RegularLatLon {
            west_lon_deg,
            east_lon_deg,
            south_lat_deg,
            north_lat_deg,
            ..
        } = self
        {
            if !(west_lon_deg < east_lon_deg && south_lat_deg < north_lat_deg) {
                return Err(VolumeStoreError::InvalidManifest(
                    "regular-lat/lon bounds are invalid".to_string(),
                ));
            }
        }
        if let Self::CurvilinearLatLon {
            nx,
            ny,
            lat_deg,
            lon_deg,
            ..
        } = self
        {
            let expected = nx
                .checked_mul(*ny)
                .ok_or_else(|| VolumeStoreError::InvalidManifest("grid size overflow".into()))?;
            if lat_deg.len() != expected || lon_deg.len() != expected {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "curvilinear grid lat/lon lengths must be {expected}, got {}/{}",
                    lat_deg.len(),
                    lon_deg.len()
                )));
            }
            if !lat_deg.iter().any(|value| value.is_finite())
                || !lon_deg.iter().any(|value| value.is_finite())
            {
                return Err(VolumeStoreError::InvalidManifest(
                    "curvilinear grid must contain finite lat/lon points".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn nx(&self) -> usize {
        match self {
            Self::RegularLatLon { nx, .. }
            | Self::LambertConformal { nx, .. }
            | Self::CurvilinearLatLon { nx, .. } => *nx,
        }
    }

    pub fn ny(&self) -> usize {
        match self {
            Self::RegularLatLon { ny, .. }
            | Self::LambertConformal { ny, .. }
            | Self::CurvilinearLatLon { ny, .. } => *ny,
        }
    }

    pub fn grid_len(&self) -> usize {
        self.nx() * self.ny()
    }

    pub fn grid_xy(&self, lat_deg: f64, lon_deg: f64) -> VolumeResult<(f32, f32)> {
        match self {
            Self::RegularLatLon {
                nx,
                ny,
                west_lon_deg,
                east_lon_deg,
                south_lat_deg,
                north_lat_deg,
            } => {
                let x = (lon_deg - *west_lon_deg) / (*east_lon_deg - *west_lon_deg)
                    * (*nx as f64 - 1.0);
                let y = (lat_deg - *south_lat_deg) / (*north_lat_deg - *south_lat_deg)
                    * (*ny as f64 - 1.0);
                if x < 0.0 || y < 0.0 || x > *nx as f64 - 1.0 || y > *ny as f64 - 1.0 {
                    return Err(VolumeStoreError::OutOfBounds(format!(
                        "lat/lon ({lat_deg:.4}, {lon_deg:.4}) outside regular grid"
                    )));
                }
                Ok((x as f32, y as f32))
            }
            Self::LambertConformal { .. } => Err(VolumeStoreError::OutOfBounds(
                "Lambert grid_xy requires the HRRR/GFS builder projection adapter".to_string(),
            )),
            Self::CurvilinearLatLon {
                nx,
                lat_deg: lat_grid,
                lon_deg: lon_grid,
                ..
            } => {
                let mut min_lat = f64::INFINITY;
                let mut max_lat = f64::NEG_INFINITY;
                let mut min_lon = f64::INFINITY;
                let mut max_lon = f64::NEG_INFINITY;
                let mut best_index = None;
                let mut best_score = f64::INFINITY;
                let cos_lat = lat_deg.to_radians().cos().abs().max(0.15);

                for (index, (&lat, &lon)) in lat_grid.iter().zip(lon_grid.iter()).enumerate() {
                    if !lat.is_finite() || !lon.is_finite() {
                        continue;
                    }
                    let lat = f64::from(lat);
                    let lon = f64::from(lon);
                    min_lat = min_lat.min(lat);
                    max_lat = max_lat.max(lat);
                    min_lon = min_lon.min(lon);
                    max_lon = max_lon.max(lon);
                    let dlat = lat - lat_deg;
                    let dlon = longitude_delta_deg(lon, lon_deg) * cos_lat;
                    let score = dlat * dlat + dlon * dlon;
                    if score < best_score {
                        best_score = score;
                        best_index = Some(index);
                    }
                }

                let Some(best_index) = best_index else {
                    return Err(VolumeStoreError::OutOfBounds(
                        "curvilinear grid has no finite points".to_string(),
                    ));
                };
                let margin_deg = 0.75;
                if lat_deg < min_lat - margin_deg
                    || lat_deg > max_lat + margin_deg
                    || lon_deg < min_lon - margin_deg
                    || lon_deg > max_lon + margin_deg
                {
                    return Err(VolumeStoreError::OutOfBounds(format!(
                        "lat/lon ({lat_deg:.4}, {lon_deg:.4}) outside curvilinear grid"
                    )));
                }
                let y = best_index / *nx;
                let x = best_index % *nx;
                Ok((x as f32, y as f32))
            }
        }
    }

    pub fn lat_lon_for_fraction(
        &self,
        fraction: f64,
        start: (f64, f64),
        end: (f64, f64),
    ) -> (f64, f64) {
        let clamped = fraction.clamp(0.0, 1.0);
        (
            start.0 + (end.0 - start.0) * clamped,
            start.1 + (end.1 - start.1) * clamped,
        )
    }
}

fn longitude_delta_deg(a: f64, b: f64) -> f64 {
    let mut delta = a - b;
    while delta > 180.0 {
        delta -= 360.0;
    }
    while delta < -180.0 {
        delta += 360.0;
    }
    delta
}
