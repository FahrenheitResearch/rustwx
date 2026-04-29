#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepAngleAxis {
    X,
    Y,
}

impl SweepAngleAxis {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "y" => Self::Y,
            _ => Self::X,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::X => "x",
            Self::Y => "y",
        }
    }
}

pub fn scan_angles_to_lat_lon(
    perspective_point_height_m: f64,
    semi_major_axis_m: f64,
    semi_minor_axis_m: f64,
    longitude_of_projection_origin_deg: f64,
    sweep_angle_axis: SweepAngleAxis,
    x_rad: f64,
    y_rad: f64,
) -> Option<(f32, f32)> {
    if !perspective_point_height_m.is_finite()
        || !semi_major_axis_m.is_finite()
        || !semi_minor_axis_m.is_finite()
        || !longitude_of_projection_origin_deg.is_finite()
        || !x_rad.is_finite()
        || !y_rad.is_finite()
    {
        return None;
    }

    let h = perspective_point_height_m + semi_major_axis_m;
    let a = semi_major_axis_m;
    let b = semi_minor_axis_m;
    if h <= 0.0 || a <= 0.0 || b <= 0.0 {
        return None;
    }

    let (x, y) = match sweep_angle_axis {
        SweepAngleAxis::X => (x_rad, y_rad),
        SweepAngleAxis::Y => (y_rad, x_rad),
    };

    let sin_x = x.sin();
    let cos_x = x.cos();
    let sin_y = y.sin();
    let cos_y = y.cos();
    let eq_to_pol = (a * a) / (b * b);

    let a_var = sin_x * sin_x + cos_x * cos_x * (cos_y * cos_y + eq_to_pol * sin_y * sin_y);
    let b_var = -2.0 * h * cos_x * cos_y;
    let c_var = h * h - a * a;
    let discriminant = b_var * b_var - 4.0 * a_var * c_var;
    if discriminant < 0.0 {
        return None;
    }

    let r_s = (-b_var - discriminant.sqrt()) / (2.0 * a_var);
    if !r_s.is_finite() || r_s <= 0.0 {
        return None;
    }

    let s_x = r_s * cos_x * cos_y;
    let s_y = -r_s * sin_x;
    let s_z = r_s * cos_x * sin_y;

    let latitude = (eq_to_pol * (s_z / ((h - s_x).hypot(s_y)))).atan();
    let longitude = longitude_of_projection_origin_deg.to_radians() - (s_y / (h - s_x)).atan();
    let lat_deg = latitude.to_degrees();
    let lon_deg = normalize_longitude_deg(longitude.to_degrees());

    if !lat_deg.is_finite() || !lon_deg.is_finite() {
        return None;
    }
    Some((lat_deg as f32, lon_deg as f32))
}

fn normalize_longitude_deg(lon: f64) -> f64 {
    let mut value = (lon + 180.0).rem_euclid(360.0) - 180.0;
    if value == -180.0 {
        value = 180.0;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: f64 = 35_786_023.0;
    const A: f64 = 6_378_137.0;
    const B: f64 = 6_356_752.314_14;
    const LON0: f64 = -137.0;

    #[test]
    fn nadir_maps_to_projection_origin() {
        let (lat, lon) = scan_angles_to_lat_lon(H, A, B, LON0, SweepAngleAxis::X, 0.0, 0.0)
            .expect("nadir should intersect earth");
        assert!(lat.abs() < 1.0e-4, "{lat}");
        assert!((f64::from(lon) - LON0).abs() < 1.0e-4, "{lon}");
    }

    #[test]
    fn far_limb_returns_none() {
        let point = scan_angles_to_lat_lon(H, A, B, LON0, SweepAngleAxis::X, 1.0, 1.0);
        assert!(point.is_none());
    }
}
