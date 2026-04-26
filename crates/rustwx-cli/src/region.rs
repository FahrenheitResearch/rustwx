#![allow(dead_code)]

use clap::ValueEnum;
use rustwx_products::shared_context::DomainSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RegionPreset {
    Midwest,
    Conus,
    California,
    CaliforniaSquare,
    RenoSquare,
    Southeast,
    SouthernPlains,
    GulfToKansas,
    Northeast,
    GreatLakes,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SplitRegionPreset {
    pub slug: &'static str,
    pub label: &'static str,
    pub bounds: (f64, f64, f64, f64),
}

impl SplitRegionPreset {
    pub fn domain(self) -> DomainSpec {
        DomainSpec::new(self.slug, self.bounds)
    }
}

pub const US_SPLIT_REGION_PRESETS: &[SplitRegionPreset] = &[
    split_region(
        "pacific_northwest",
        "Pacific Northwest",
        (-125.0, -110.0, 41.0, 49.5),
    ),
    split_region(
        "california_southwest",
        "California / Southwest",
        (-125.0, -108.0, 31.0, 41.5),
    ),
    split_region(
        "rockies_high_plains",
        "Rockies / High Plains",
        (-112.0, -96.0, 37.0, 49.5),
    ),
    split_region(
        "southern_plains",
        "Southern Plains",
        (-109.0, -90.0, 25.0, 40.5),
    ),
    split_region("great_lakes", "Great Lakes", (-97.5, -72.0, 39.0, 50.5)),
    split_region("southeast", "Southeast", (-96.0, -72.0, 24.0, 38.5)),
    split_region("northeast", "Northeast", (-84.5, -65.0, 36.0, 48.5)),
];

pub fn us_split_region_domains() -> Vec<DomainSpec> {
    US_SPLIT_REGION_PRESETS
        .iter()
        .copied()
        .map(SplitRegionPreset::domain)
        .collect()
}

pub fn conus_plus_us_split_region_domains() -> Vec<DomainSpec> {
    let mut domains = Vec::with_capacity(1 + US_SPLIT_REGION_PRESETS.len());
    domains.push(DomainSpec::new(
        RegionPreset::Conus.slug(),
        RegionPreset::Conus.bounds(),
    ));
    domains.extend(us_split_region_domains());
    domains
}

const fn split_region(
    slug: &'static str,
    label: &'static str,
    bounds: (f64, f64, f64, f64),
) -> SplitRegionPreset {
    SplitRegionPreset {
        slug,
        label,
        bounds,
    }
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
            Self::GulfToKansas => (-103.5, -90.0, 25.0, 40.5),
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
            Self::GulfToKansas => "gulf_to_kansas",
            Self::Northeast => "northeast",
            Self::GreatLakes => "great_lakes",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RegionPreset, US_SPLIT_REGION_PRESETS};
    use std::collections::HashSet;

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

    #[test]
    fn split_region_slugs_are_unique() {
        let mut seen = HashSet::new();
        for region in US_SPLIT_REGION_PRESETS {
            assert!(
                seen.insert(region.slug),
                "duplicate split-region slug {}",
                region.slug
            );
        }
    }
}
