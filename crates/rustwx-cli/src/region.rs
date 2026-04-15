use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RegionPreset {
    Midwest,
    Conus,
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
            Self::Southeast => "southeast",
            Self::SouthernPlains => "southern_plains",
            Self::Northeast => "northeast",
            Self::GreatLakes => "great_lakes",
        }
    }
}
