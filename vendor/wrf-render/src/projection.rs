//! Lambert Conformal Conic projection math for WRF grids.

use crate::overlay::MapExtent;
use std::f64::consts::PI;

const R_EARTH: f64 = 6_370_000.0;
const DEG2RAD: f64 = PI / 180.0;

/// Lambert Conformal Conic projection.
pub struct LambertConformal {
    n: f64,
    f: f64,
    rho0: f64,
    lambda0: f64,
}

impl LambertConformal {
    /// Create from WRF parameters (all in degrees).
    pub fn new(truelat1: f64, truelat2: f64, stand_lon: f64, ref_lat: f64) -> Self {
        let phi1 = truelat1 * DEG2RAD;
        let phi2 = truelat2 * DEG2RAD;
        let phi0 = ref_lat * DEG2RAD;
        let lambda0 = stand_lon * DEG2RAD;

        let n = if (truelat1 - truelat2).abs() < 1e-10 {
            phi1.sin()
        } else {
            let num = (phi1.cos()).ln() - (phi2.cos()).ln();
            let den = ((PI / 4.0 + phi2 / 2.0).tan()).ln() - ((PI / 4.0 + phi1 / 2.0).tan()).ln();
            num / den
        };

        let f = phi1.cos() * (PI / 4.0 + phi1 / 2.0).tan().powf(n) / n;
        let rho0 = R_EARTH * f / (PI / 4.0 + phi0 / 2.0).tan().powf(n);

        Self {
            n,
            f,
            rho0,
            lambda0,
        }
    }

    pub fn project(&self, lat: f64, lon: f64) -> (f64, f64) {
        let phi = lat * DEG2RAD;
        let lambda = lon * DEG2RAD;

        let rho = R_EARTH * self.f / (PI / 4.0 + phi / 2.0).tan().powf(self.n);
        let theta = self.n * (lambda - self.lambda0);

        let x = rho * theta.sin();
        let y = self.rho0 - rho * theta.cos();
        (x, y)
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
        let data_ratio = data_width / data_height.max(1e-12);

        if data_ratio > target_ratio {
            let new_height = data_width / target_ratio;
            let pad_y = (new_height - data_height) / 2.0;
            Self {
                x_min,
                x_max,
                y_min: y_min - pad_y,
                y_max: y_max + pad_y,
            }
        } else {
            let new_width = data_height * target_ratio;
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
