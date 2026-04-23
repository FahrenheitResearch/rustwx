use rustwx_products::places::{self, PlacePreset, PlaceSelectionOptions};
use rustwx_products::shared_context::DomainSpec;

pub type MetroCropPreset = places::MetroCropPreset;

pub const MAJOR_US_CITY_PRESETS: &[PlacePreset] = places::MAJOR_US_CITY_PRESETS;
pub const CITY_OUTPUT_ASPECT_RATIO: f64 = places::PLACE_OUTPUT_ASPECT_RATIO;

pub fn centered_domain<S: Into<String>>(
    slug: S,
    center_lon: f64,
    center_lat: f64,
    half_height_deg: f64,
) -> DomainSpec {
    places::centered_domain(slug, center_lon, center_lat, half_height_deg)
}

pub fn major_us_city_domains() -> Vec<DomainSpec> {
    places::major_us_city_domains()
}

pub fn select_major_us_city_domains(
    bounds: (f64, f64, f64, f64),
    options: PlaceSelectionOptions,
) -> Vec<DomainSpec> {
    places::select_major_us_city_domains(bounds, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn major_city_slugs_are_unique() {
        let mut seen = HashSet::new();
        for city in MAJOR_US_CITY_PRESETS {
            assert!(seen.insert(city.slug), "duplicate city slug {}", city.slug);
        }
    }

    #[test]
    fn new_york_city_bounds_stay_centered() {
        let domain = MAJOR_US_CITY_PRESETS
            .iter()
            .find(|city| city.slug == "ny_new_york_city")
            .expect("NYC preset should exist")
            .domain();
        let (west, east, south, north) = domain.bounds;
        assert!((((west + east) / 2.0) + 74.0).abs() < 0.1);
        assert!((((south + north) / 2.0) - 40.71).abs() < 0.1);
    }

    #[test]
    fn centered_domain_preserves_requested_physical_aspect_ratio() {
        let domain = centered_domain("custom_poi", -104.99, 39.74, 1.9);
        let (west, east, south, north) = domain.bounds;
        let center_lat = (south + north) / 2.0;
        let width_deg = (east - west) * center_lat.to_radians().cos().abs();
        let height_deg = north - south;
        let aspect_ratio = width_deg / height_deg;

        assert!((aspect_ratio - CITY_OUTPUT_ASPECT_RATIO).abs() < 1.0e-6);
    }

    #[test]
    fn major_us_city_domains_follow_preset_order() {
        let domains = major_us_city_domains();

        assert_eq!(domains.len(), MAJOR_US_CITY_PRESETS.len());
        assert_eq!(domains[0].slug, MAJOR_US_CITY_PRESETS[0].slug);
        assert_eq!(
            domains.last().map(|domain| domain.slug.as_str()),
            Some(MAJOR_US_CITY_PRESETS.last().unwrap().slug)
        );
    }
}
