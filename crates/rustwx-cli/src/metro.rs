use rustwx_products::shared_context::DomainSpec;

const DEFAULT_CITY_HALF_HEIGHT_DEG: f64 = 1.9;
const CITY_OUTPUT_ASPECT_RATIO: f64 = 1200.0 / 900.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetroCropPreset {
    pub slug: &'static str,
    pub label: &'static str,
    pub center_lon: f64,
    pub center_lat: f64,
    pub half_height_deg: f64,
}

impl MetroCropPreset {
    pub fn bounds(self) -> (f64, f64, f64, f64) {
        centered_bounds(
            self.center_lon,
            self.center_lat,
            self.half_height_deg,
            CITY_OUTPUT_ASPECT_RATIO,
        )
    }

    pub fn domain(self) -> DomainSpec {
        DomainSpec::new(self.slug, self.bounds())
    }
}

pub const MAJOR_US_CITY_PRESETS: &[MetroCropPreset] = &[
    metro("al_birmingham", "Birmingham, AL", -86.80, 33.52),
    metro("ak_anchorage", "Anchorage, AK", -149.90, 61.22),
    metro("az_phoenix", "Phoenix, AZ", -112.07, 33.45),
    metro("ar_little_rock", "Little Rock, AR", -92.29, 34.75),
    metro("ca_los_angeles", "Los Angeles, CA", -118.24, 34.05),
    metro("ca_san_francisco_bay", "San Francisco Bay, CA", -122.27, 37.80),
    metro("ca_sacramento", "Sacramento, CA", -121.49, 38.58),
    metro("ca_san_diego", "San Diego, CA", -117.16, 32.72),
    metro("co_denver", "Denver, CO", -104.99, 39.74),
    metro("ct_hartford", "Hartford, CT", -72.67, 41.77),
    metro("de_wilmington", "Wilmington, DE", -75.55, 39.74),
    metro("dc_washington", "Washington, DC", -77.04, 38.91),
    metro("fl_miami", "Miami, FL", -80.19, 25.76),
    metro("fl_tampa", "Tampa, FL", -82.46, 27.95),
    metro("fl_orlando", "Orlando, FL", -81.38, 28.54),
    metro("ga_atlanta", "Atlanta, GA", -84.39, 33.75),
    metro("hi_honolulu", "Honolulu, HI", -157.86, 21.31),
    metro("id_boise", "Boise, ID", -116.20, 43.62),
    metro("il_chicago", "Chicago, IL", -87.63, 41.88),
    metro("in_indianapolis", "Indianapolis, IN", -86.16, 39.77),
    metro("ia_des_moines", "Des Moines, IA", -93.62, 41.59),
    metro("ks_wichita", "Wichita, KS", -97.34, 37.69),
    metro("ky_louisville", "Louisville, KY", -85.76, 38.25),
    metro("la_new_orleans", "New Orleans, LA", -90.07, 29.95),
    metro("me_portland", "Portland, ME", -70.26, 43.66),
    metro("md_baltimore", "Baltimore, MD", -76.61, 39.29),
    metro("ma_boston", "Boston, MA", -71.06, 42.36),
    metro("mi_detroit", "Detroit, MI", -83.05, 42.33),
    metro("mn_minneapolis", "Minneapolis, MN", -93.27, 44.98),
    metro("ms_jackson", "Jackson, MS", -90.18, 32.30),
    metro("mo_st_louis", "St. Louis, MO", -90.20, 38.63),
    metro("mt_billings", "Billings, MT", -108.50, 45.78),
    metro("ne_omaha", "Omaha, NE", -95.94, 41.26),
    metro("nv_las_vegas", "Las Vegas, NV", -115.14, 36.17),
    metro("nv_reno", "Reno, NV", -119.81, 39.53),
    metro("nh_manchester", "Manchester, NH", -71.45, 42.99),
    metro("nj_newark", "Newark, NJ", -74.17, 40.74),
    metro("nm_albuquerque", "Albuquerque, NM", -106.65, 35.08),
    metro("ny_new_york_city", "New York City, NY", -74.00, 40.71),
    metro("nc_charlotte", "Charlotte, NC", -80.84, 35.23),
    metro("nd_fargo", "Fargo, ND", -96.79, 46.88),
    metro("oh_columbus", "Columbus, OH", -82.99, 39.96),
    metro("ok_oklahoma_city", "Oklahoma City, OK", -97.52, 35.47),
    metro("or_portland", "Portland, OR", -122.68, 45.52),
    metro("pa_philadelphia", "Philadelphia, PA", -75.17, 39.95),
    metro("ri_providence", "Providence, RI", -71.41, 41.82),
    metro("sc_charleston", "Charleston, SC", -79.93, 32.78),
    metro("sd_sioux_falls", "Sioux Falls, SD", -96.73, 43.55),
    metro("tn_nashville", "Nashville, TN", -86.78, 36.16),
    metro("tx_dallas_fort_worth", "Dallas-Fort Worth, TX", -97.04, 32.90),
    metro("tx_houston", "Houston, TX", -95.37, 29.76),
    metro("tx_austin", "Austin, TX", -97.74, 30.27),
    metro("tx_san_antonio", "San Antonio, TX", -98.49, 29.42),
    metro("ut_salt_lake_city", "Salt Lake City, UT", -111.89, 40.76),
    metro("vt_burlington", "Burlington, VT", -73.21, 44.48),
    metro("va_richmond", "Richmond, VA", -77.44, 37.54),
    metro("wa_seattle", "Seattle, WA", -122.33, 47.61),
    metro("wv_charleston", "Charleston, WV", -81.63, 38.35),
    metro("wi_milwaukee", "Milwaukee, WI", -87.91, 43.04),
    metro("wy_cheyenne", "Cheyenne, WY", -104.82, 41.14),
];

pub fn major_us_city_domains() -> Vec<DomainSpec> {
    MAJOR_US_CITY_PRESETS.iter().copied().map(|city| city.domain()).collect()
}

const fn metro(
    slug: &'static str,
    label: &'static str,
    center_lon: f64,
    center_lat: f64,
) -> MetroCropPreset {
    MetroCropPreset {
        slug,
        label,
        center_lon,
        center_lat,
        half_height_deg: DEFAULT_CITY_HALF_HEIGHT_DEG,
    }
}

fn centered_bounds(
    center_lon: f64,
    center_lat: f64,
    half_height_deg: f64,
    aspect_ratio: f64,
) -> (f64, f64, f64, f64) {
    let cos_lat = center_lat.to_radians().cos().abs().max(0.25);
    let half_width_deg = half_height_deg * aspect_ratio / cos_lat;
    (
        center_lon - half_width_deg,
        center_lon + half_width_deg,
        center_lat - half_height_deg,
        center_lat + half_height_deg,
    )
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
        let domain = MAJOR_US_CITY_PRESETS[0].domain();
        let (west, east, south, north) = domain.bounds;
        assert!((((west + east) / 2.0) + 74.0).abs() < 0.1);
        assert!(((((south + north) / 2.0) - 40.71)).abs() < 0.1);
    }
}
