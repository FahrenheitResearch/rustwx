use crate::places::{self, PlacePreset};
use crate::shared_context::DomainSpec;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

const GROUP_US_REGION: &str = "us_region";
const GROUP_US_SPLIT_REGION: &str = "us_split_region";
const GROUP_US_MAJOR_METRO: &str = "us_major_metro";
const GROUP_CROSS_SECTION_ROUTE: &str = "cross_section_proof_route";

const TAG_US_REGION: &[&str] = &["us", "region"];
const TAG_US_SPLIT_REGION: &[&str] = &["us", "region", "split"];
const TAG_US_MAJOR_METRO: &[&str] = &["us", "metro", "major"];
const TAG_CROSS_SECTION_ROUTE: &[&str] = &["route", "cross_section", "fixed"];

const GROUPS_US_REGION: &[&str] = &[GROUP_US_REGION];
const GROUPS_US_SPLIT_REGION: &[&str] = &[GROUP_US_SPLIT_REGION];
const GROUPS_US_REGION_AND_SPLIT: &[&str] = &[GROUP_US_REGION, GROUP_US_SPLIT_REGION];
const GROUPS_CROSS_SECTION_ROUTE: &[&str] = &[GROUP_CROSS_SECTION_ROUTE];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedGeometryKind {
    Metro,
    Region,
    WatchArea,
    Route,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NamedGeoPoint {
    pub lat_deg: f64,
    pub lon_deg: f64,
}

impl NamedGeoPoint {
    pub const fn new(lat_deg: f64, lon_deg: f64) -> Self {
        Self { lat_deg, lon_deg }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NamedGeoBounds {
    pub west_deg: f64,
    pub east_deg: f64,
    pub south_deg: f64,
    pub north_deg: f64,
}

impl NamedGeoBounds {
    pub const fn new(west_deg: f64, east_deg: f64, south_deg: f64, north_deg: f64) -> Self {
        Self {
            west_deg,
            east_deg,
            south_deg,
            north_deg,
        }
    }

    pub const fn as_tuple(self) -> (f64, f64, f64, f64) {
        (self.west_deg, self.east_deg, self.south_deg, self.north_deg)
    }

    pub fn center(self) -> NamedGeoPoint {
        NamedGeoPoint::new(
            (self.south_deg + self.north_deg) / 2.0,
            (self.west_deg + self.east_deg) / 2.0,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "geometry_type", rename_all = "snake_case")]
pub enum NamedGeometry {
    Bounds {
        bounds: NamedGeoBounds,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        center: Option<NamedGeoPoint>,
    },
    Path {
        points: Vec<NamedGeoPoint>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedGeometryAsset {
    pub slug: String,
    pub label: String,
    pub kind: NamedGeometryKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    pub geometry: NamedGeometry,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

impl NamedGeometryAsset {
    pub fn bounds<S1: Into<String>, S2: Into<String>>(
        slug: S1,
        label: S2,
        kind: NamedGeometryKind,
        bounds: NamedGeoBounds,
    ) -> Self {
        Self {
            slug: slug.into(),
            label: label.into(),
            kind,
            groups: Vec::new(),
            geometry: NamedGeometry::Bounds {
                bounds,
                center: None,
            },
            tags: Vec::new(),
        }
    }

    pub fn route<S1: Into<String>, S2: Into<String>>(
        slug: S1,
        label: S2,
        points: Vec<NamedGeoPoint>,
    ) -> Self {
        Self {
            slug: slug.into(),
            label: label.into(),
            kind: NamedGeometryKind::Route,
            groups: Vec::new(),
            geometry: NamedGeometry::Path { points },
            tags: Vec::new(),
        }
    }

    pub fn with_center(mut self, center: NamedGeoPoint) -> Self {
        if let NamedGeometry::Bounds {
            center: existing, ..
        } = &mut self.geometry
        {
            *existing = Some(center);
        }
        self
    }

    pub fn with_group<S: Into<String>>(mut self, group: S) -> Self {
        push_unique_string(&mut self.groups, group.into());
        self
    }

    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags.clear();
        for tag in tags {
            push_unique_string(&mut self.tags, tag.into());
        }
        self
    }

    pub fn bounds_geometry(&self) -> Option<NamedGeoBounds> {
        match self.geometry {
            NamedGeometry::Bounds { bounds, .. } => Some(bounds),
            NamedGeometry::Path { .. } => None,
        }
    }

    pub fn path_points(&self) -> Option<&[NamedGeoPoint]> {
        match &self.geometry {
            NamedGeometry::Bounds { .. } => None,
            NamedGeometry::Path { points } => Some(points.as_slice()),
        }
    }

    pub fn domain_spec(&self) -> Option<DomainSpec> {
        self.bounds_geometry()
            .map(|bounds| DomainSpec::new(self.slug.clone(), bounds.as_tuple()))
    }

    pub fn has_group(&self, group: &str) -> bool {
        self.groups.iter().any(|candidate| candidate == group)
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|candidate| candidate == tag)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NamedGeometrySelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<NamedGeometryKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub slugs: Vec<String>,
}

impl NamedGeometrySelector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_kind(mut self, kind: NamedGeometryKind) -> Self {
        self.kind = Some(kind);
        self
    }

    pub fn with_group<S: Into<String>>(mut self, group: S) -> Self {
        self.group = Some(group.into());
        self
    }

    pub fn with_tag<S: Into<String>>(mut self, tag: S) -> Self {
        push_unique_string(&mut self.tags, tag.into());
        self
    }

    pub fn with_slug<S: Into<String>>(mut self, slug: S) -> Self {
        push_unique_string(&mut self.slugs, slug.into());
        self
    }

    fn matches(&self, asset: &NamedGeometryAsset) -> bool {
        if let Some(kind) = self.kind {
            if asset.kind != kind {
                return false;
            }
        }
        if let Some(group) = self.group.as_deref() {
            if !asset.has_group(group) {
                return false;
            }
        }
        if !self.slugs.is_empty() && !self.slugs.iter().any(|slug| slug == &asset.slug) {
            return false;
        }
        self.tags.iter().all(|tag| asset.has_tag(tag))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct NamedGeometryCatalog {
    pub assets: Vec<NamedGeometryAsset>,
}

impl NamedGeometryCatalog {
    pub fn new(assets: Vec<NamedGeometryAsset>) -> Self {
        Self { assets }
    }

    pub fn built_in() -> Self {
        built_in_named_geometry_catalog()
    }

    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    pub fn from_json_str(value: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(value)
    }

    pub fn load_json(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = fs::read(path)?;
        Ok(Self::from_json_slice(&bytes)?)
    }

    pub fn len(&self) -> usize {
        self.assets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assets.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &NamedGeometryAsset> {
        self.assets.iter()
    }

    pub fn find(&self, slug: &str) -> Option<&NamedGeometryAsset> {
        self.assets.iter().find(|asset| asset.slug == slug)
    }

    pub fn of_kind(&self, kind: NamedGeometryKind) -> Vec<&NamedGeometryAsset> {
        self.assets
            .iter()
            .filter(|asset| asset.kind == kind)
            .collect()
    }

    pub fn select<'a>(&'a self, selector: &NamedGeometrySelector) -> Vec<&'a NamedGeometryAsset> {
        self.assets
            .iter()
            .filter(|asset| selector.matches(asset))
            .collect()
    }

    pub fn domain_specs(&self, selector: &NamedGeometrySelector) -> Vec<DomainSpec> {
        self.select(selector)
            .into_iter()
            .filter_map(|asset| asset.domain_spec())
            .collect()
    }
}

pub fn built_in_named_geometry_catalog() -> NamedGeometryCatalog {
    let mut assets = Vec::new();
    assets.extend(built_in_region_assets());
    assets.extend(built_in_metro_assets());
    assets.extend(built_in_watch_area_assets());
    assets.extend(built_in_route_assets());
    NamedGeometryCatalog::new(assets)
}

pub fn built_in_named_geometry_assets() -> Vec<NamedGeometryAsset> {
    built_in_named_geometry_catalog().assets
}

pub fn built_in_region_assets() -> Vec<NamedGeometryAsset> {
    BUILT_IN_ALL_REGION_PRESETS
        .iter()
        .copied()
        .map(BuiltInBoundsPreset::to_asset)
        .collect()
}

pub fn built_in_standard_region_assets() -> Vec<NamedGeometryAsset> {
    built_in_region_assets()
        .into_iter()
        .filter(|asset| asset.has_group(GROUP_US_REGION))
        .collect()
}

pub fn built_in_split_region_assets() -> Vec<NamedGeometryAsset> {
    built_in_region_assets()
        .into_iter()
        .filter(|asset| asset.has_group(GROUP_US_SPLIT_REGION))
        .collect()
}

pub fn built_in_region_domains() -> Vec<DomainSpec> {
    built_in_region_assets()
        .into_iter()
        .filter_map(|asset| asset.domain_spec())
        .collect()
}

pub fn built_in_standard_region_domains() -> Vec<DomainSpec> {
    built_in_standard_region_assets()
        .into_iter()
        .filter_map(|asset| asset.domain_spec())
        .collect()
}

pub fn built_in_split_region_domains() -> Vec<DomainSpec> {
    built_in_split_region_assets()
        .into_iter()
        .filter_map(|asset| asset.domain_spec())
        .collect()
}

pub fn built_in_metro_assets() -> Vec<NamedGeometryAsset> {
    places::major_us_city_places()
        .iter()
        .copied()
        .map(metro_asset_from_place)
        .collect()
}

pub fn built_in_watch_area_assets() -> Vec<NamedGeometryAsset> {
    Vec::new()
}

pub fn built_in_route_assets() -> Vec<NamedGeometryAsset> {
    BUILT_IN_ROUTE_PRESETS
        .iter()
        .copied()
        .map(BuiltInRoutePreset::to_asset)
        .collect()
}

pub fn find_built_in_named_geometry(slug: &str) -> Option<NamedGeometryAsset> {
    built_in_named_geometry_catalog().find(slug).cloned()
}

fn metro_asset_from_place(place: PlacePreset) -> NamedGeometryAsset {
    NamedGeometryAsset::bounds(
        place.slug,
        place.label,
        NamedGeometryKind::Metro,
        NamedGeoBounds::from(place.bounds()),
    )
    .with_center(NamedGeoPoint::new(place.center_lat, place.center_lon))
    .with_group(GROUP_US_MAJOR_METRO)
    .with_tags(TAG_US_MAJOR_METRO.iter().copied())
}

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if value.is_empty() || values.iter().any(|existing| existing == &value) {
        return;
    }
    values.push(value);
}

impl From<(f64, f64, f64, f64)> for NamedGeoBounds {
    fn from(value: (f64, f64, f64, f64)) -> Self {
        Self::new(value.0, value.1, value.2, value.3)
    }
}

#[derive(Debug, Clone, Copy)]
struct BuiltInBoundsPreset {
    slug: &'static str,
    label: &'static str,
    kind: NamedGeometryKind,
    groups: &'static [&'static str],
    tags: &'static [&'static str],
    bounds: NamedGeoBounds,
}

impl BuiltInBoundsPreset {
    fn to_asset(self) -> NamedGeometryAsset {
        let mut asset = NamedGeometryAsset::bounds(self.slug, self.label, self.kind, self.bounds)
            .with_tags(self.tags.iter().copied());
        for group in self.groups {
            asset = asset.with_group(*group);
        }
        asset
    }
}

#[derive(Debug, Clone, Copy)]
struct BuiltInRoutePreset {
    slug: &'static str,
    label: &'static str,
    groups: &'static [&'static str],
    tags: &'static [&'static str],
    points: &'static [NamedGeoPoint],
}

impl BuiltInRoutePreset {
    fn to_asset(self) -> NamedGeometryAsset {
        let mut asset = NamedGeometryAsset::route(self.slug, self.label, self.points.to_vec())
            .with_tags(self.tags.iter().copied());
        for group in self.groups {
            asset = asset.with_group(*group);
        }
        asset
    }
}

const BUILT_IN_ALL_REGION_PRESETS: &[BuiltInBoundsPreset] = &[
    BuiltInBoundsPreset {
        slug: "midwest",
        label: "Midwest",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION,
        tags: TAG_US_REGION,
        bounds: NamedGeoBounds::new(-104.0, -74.0, 28.0, 49.0),
    },
    BuiltInBoundsPreset {
        slug: "conus",
        label: "CONUS",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION,
        tags: TAG_US_REGION,
        bounds: NamedGeoBounds::new(-127.0, -66.0, 23.0, 51.5),
    },
    BuiltInBoundsPreset {
        slug: "california",
        label: "California",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION,
        tags: TAG_US_REGION,
        bounds: NamedGeoBounds::new(-124.9, -113.8, 31.9, 42.5),
    },
    BuiltInBoundsPreset {
        slug: "california_square",
        label: "California Square",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION,
        tags: TAG_US_REGION,
        bounds: NamedGeoBounds::new(-124.9, -113.7, 31.8, 42.7),
    },
    BuiltInBoundsPreset {
        slug: "reno_square",
        label: "Reno Square",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION,
        tags: TAG_US_REGION,
        bounds: NamedGeoBounds::new(-123.1, -116.1, 36.1, 43.1),
    },
    BuiltInBoundsPreset {
        slug: "southeast",
        label: "Southeast",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION_AND_SPLIT,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-96.0, -72.0, 24.0, 38.5),
    },
    BuiltInBoundsPreset {
        slug: "southern_plains",
        label: "Southern Plains",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION_AND_SPLIT,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-109.0, -90.0, 25.0, 40.5),
    },
    BuiltInBoundsPreset {
        slug: "northeast",
        label: "Northeast",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION_AND_SPLIT,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-84.5, -65.0, 36.0, 48.5),
    },
    BuiltInBoundsPreset {
        slug: "great_lakes",
        label: "Great Lakes",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_REGION_AND_SPLIT,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-97.5, -72.0, 39.0, 50.5),
    },
    BuiltInBoundsPreset {
        slug: "pacific_northwest",
        label: "Pacific Northwest",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_SPLIT_REGION,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-125.0, -110.0, 41.0, 49.5),
    },
    BuiltInBoundsPreset {
        slug: "california_southwest",
        label: "California / Southwest",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_SPLIT_REGION,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-125.0, -108.0, 31.0, 41.5),
    },
    BuiltInBoundsPreset {
        slug: "rockies_high_plains",
        label: "Rockies / High Plains",
        kind: NamedGeometryKind::Region,
        groups: GROUPS_US_SPLIT_REGION,
        tags: TAG_US_SPLIT_REGION,
        bounds: NamedGeoBounds::new(-112.0, -96.0, 37.0, 49.5),
    },
];

const AMARILLO_CHICAGO_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(35.2220, -101.8313),
    NamedGeoPoint::new(41.8781, -87.6298),
];
const KANSAS_CITY_CHICAGO_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(39.0997, -94.5786),
    NamedGeoPoint::new(41.8781, -87.6298),
];
const SAN_FRANCISCO_TAHOE_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(37.8044, -122.2712),
    NamedGeoPoint::new(38.9399, -119.9772),
];
const SACRAMENTO_RENO_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(38.5816, -121.4944),
    NamedGeoPoint::new(39.5296, -119.8138),
];
const LOS_ANGELES_MOJAVE_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(34.0522, -118.2437),
    NamedGeoPoint::new(35.0525, -118.1739),
];
const SAN_DIEGO_IMPERIAL_POINTS: [NamedGeoPoint; 2] = [
    NamedGeoPoint::new(32.7157, -117.1611),
    NamedGeoPoint::new(32.7920, -115.5631),
];

const BUILT_IN_ROUTE_PRESETS: &[BuiltInRoutePreset] = &[
    BuiltInRoutePreset {
        slug: "amarillo_chicago",
        label: "Amarillo to Chicago",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &AMARILLO_CHICAGO_POINTS,
    },
    BuiltInRoutePreset {
        slug: "kansas_city_chicago",
        label: "Kansas City to Chicago",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &KANSAS_CITY_CHICAGO_POINTS,
    },
    BuiltInRoutePreset {
        slug: "san_francisco_tahoe",
        label: "San Francisco to Tahoe",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &SAN_FRANCISCO_TAHOE_POINTS,
    },
    BuiltInRoutePreset {
        slug: "sacramento_reno",
        label: "Sacramento to Reno",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &SACRAMENTO_RENO_POINTS,
    },
    BuiltInRoutePreset {
        slug: "los_angeles_mojave",
        label: "Los Angeles to Mojave",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &LOS_ANGELES_MOJAVE_POINTS,
    },
    BuiltInRoutePreset {
        slug: "san_diego_imperial",
        label: "San Diego to Imperial",
        groups: GROUPS_CROSS_SECTION_ROUTE,
        tags: TAG_CROSS_SECTION_ROUTE,
        points: &SAN_DIEGO_IMPERIAL_POINTS,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn built_in_catalog_spans_regions_metros_and_routes() {
        let catalog = NamedGeometryCatalog::built_in();
        let kinds = catalog
            .iter()
            .map(|asset| asset.kind)
            .collect::<HashSet<_>>();

        assert!(kinds.contains(&NamedGeometryKind::Region));
        assert!(kinds.contains(&NamedGeometryKind::Metro));
        assert!(kinds.contains(&NamedGeometryKind::Route));
    }

    #[test]
    fn built_in_catalog_slugs_are_unique() {
        let catalog = NamedGeometryCatalog::built_in();
        let mut seen = HashSet::new();

        for asset in catalog.iter() {
            assert!(
                seen.insert(asset.slug.as_str()),
                "duplicate named geometry slug {}",
                asset.slug
            );
        }
    }

    #[test]
    fn selector_filters_by_kind_group_and_tag() {
        let catalog = NamedGeometryCatalog::built_in();
        let selector = NamedGeometrySelector::new()
            .with_kind(NamedGeometryKind::Region)
            .with_group(GROUP_US_SPLIT_REGION)
            .with_tag("split");
        let selected = catalog.select(&selector);

        assert!(!selected.is_empty());
        assert!(
            selected
                .iter()
                .all(|asset| asset.kind == NamedGeometryKind::Region)
        );
        assert!(
            selected
                .iter()
                .all(|asset| asset.has_group(GROUP_US_SPLIT_REGION))
        );
        assert!(selected.iter().all(|asset| asset.has_tag("split")));
    }

    #[test]
    fn domain_specs_skip_route_assets() {
        let catalog = NamedGeometryCatalog::built_in();
        let selector = NamedGeometrySelector::new().with_slug("amarillo_chicago");

        assert!(catalog.domain_specs(&selector).is_empty());
    }

    #[test]
    fn json_loader_supports_external_watch_area_catalogs() {
        let catalog = NamedGeometryCatalog::from_json_str(
            r#"{
                "assets": [
                    {
                        "slug": "foothill_watch",
                        "label": "Foothill Watch",
                        "kind": "watch_area",
                        "groups": ["enterprise_watch"],
                        "geometry": {
                            "geometry_type": "bounds",
                            "bounds": {
                                "west_deg": -122.5,
                                "east_deg": -121.5,
                                "south_deg": 38.1,
                                "north_deg": 39.0
                            }
                        },
                        "tags": ["enterprise", "fire"]
                    }
                ]
            }"#,
        )
        .expect("watch area catalog should deserialize");
        let asset = catalog
            .find("foothill_watch")
            .expect("watch area slug should be present");

        assert_eq!(asset.kind, NamedGeometryKind::WatchArea);
        assert!(asset.has_group("enterprise_watch"));
        assert_eq!(
            asset
                .domain_spec()
                .expect("watch area bounds should map to a domain"),
            DomainSpec::new("foothill_watch", (-122.5, -121.5, 38.1, 39.0))
        );
    }

    #[test]
    fn metro_assets_preserve_place_centers() {
        let preset = places::major_us_city_places()
            .iter()
            .find(|candidate| candidate.slug == "ca_los_angeles")
            .expect("Los Angeles metro preset should exist");
        let asset = built_in_metro_assets()
            .into_iter()
            .find(|candidate| candidate.slug == "ca_los_angeles")
            .expect("Los Angeles metro should exist");

        match asset.geometry {
            NamedGeometry::Bounds {
                center: Some(center),
                ..
            } => {
                assert!((center.lat_deg - preset.center_lat).abs() < 1.0e-6);
                assert!((center.lon_deg - preset.center_lon).abs() < 1.0e-6);
            }
            _ => panic!("metro geometry should carry a center point"),
        }
    }
}
