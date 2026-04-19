use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RegionPreset {
    Midwest,
    Conus,
    California,
    CaliforniaSquare,
    RenoSquare,
    Southeast,
    SouthernPlains,
    Northeast,
    GreatLakes,
}

impl RegionPreset {
    pub fn bounds(self) -> (f64, f64, f64, f64) {
        match self {
            Self::Midwest => (-104.0, -74.0, 28.0, 49.0),
            Self::Conus => (-127.0, -66.0, 23.0, 51.5),
            Self::California => (-124.9, -113.8, 31.9, 42.5),
            Self::CaliforniaSquare => (-124.9, -113.7, 31.8, 42.7),
            Self::RenoSquare => (-123.1, -116.1, 36.1, 43.1),
            Self::Southeast => (-96.0, -72.0, 24.0, 38.5),
            Self::SouthernPlains => (-109.0, -90.0, 25.0, 40.5),
            Self::Northeast => (-84.5, -65.0, 36.0, 48.5),
            Self::GreatLakes => (-97.5, -72.0, 39.0, 50.5),
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Midwest => "midwest",
            Self::Conus => "conus",
            Self::California => "california",
            Self::CaliforniaSquare => "california_square",
            Self::RenoSquare => "reno_square",
            Self::Southeast => "southeast",
            Self::SouthernPlains => "southern_plains",
            Self::Northeast => "northeast",
            Self::GreatLakes => "great_lakes",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RegionPreset;

    #[test]
    fn california_square_contains_california_bounds() {
        let ca = RegionPreset::California.bounds();
        let square = RegionPreset::CaliforniaSquare.bounds();
        assert!(square.0 <= ca.0);
        assert!(square.1 >= ca.1);
        assert!(square.2 <= ca.2);
        assert!(square.3 >= ca.3);
    }

    #[test]
    fn california_square_slug_is_stable() {
        assert_eq!(RegionPreset::CaliforniaSquare.slug(), "california_square");
    }

    #[test]
    fn reno_square_is_centered_near_reno() {
        let (west, east, south, north) = RegionPreset::RenoSquare.bounds();
        let center_lon = (west + east) / 2.0;
        let center_lat = (south + north) / 2.0;
        assert!((center_lon + 119.8).abs() < 0.5);
        assert!((center_lat - 39.5).abs() < 0.5);
    }
}
