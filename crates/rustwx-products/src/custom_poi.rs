use crate::shared_context::DomainSpec;
use rustwx_core::GridProjection;
use rustwx_render::{
    Color, LineworkRole, MapRenderRequest, ProjectedDomainBuildOptions, ProjectedLineOverlay,
    ProjectionSpec, build_projected_domain,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

const DEFAULT_CAMERA_CLUSTER_ASPECT_RATIO: f64 = 1200.0 / 900.0;
const KM_PER_DEG_LAT: f64 = 111.32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectedPointAnnotationKind {
    Camera,
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedPointAnnotationStyle {
    pub marker_radius_px: f64,
    pub marker_fill: Color,
    pub marker_outline: Color,
    pub marker_outline_width: u32,
    pub arrow_color: Color,
    pub arrow_length_px: f64,
    pub arrow_width: u32,
    pub label_color: Color,
    pub label_halo: Color,
    pub label_offset_x_px: i32,
    pub label_offset_y_px: i32,
    pub label_scale: u32,
}

impl Default for ProjectedPointAnnotationStyle {
    fn default() -> Self {
        Self {
            marker_radius_px: 6.0,
            marker_fill: Color::rgba(255, 191, 71, 255),
            marker_outline: Color::BLACK,
            marker_outline_width: 2,
            arrow_color: Color::BLACK,
            arrow_length_px: 28.0,
            arrow_width: 2,
            label_color: Color::BLACK,
            label_halo: Color::WHITE,
            label_offset_x_px: 10,
            label_offset_y_px: -14,
            label_scale: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedPointAnnotation {
    pub x: f64,
    pub y: f64,
    pub kind: ProjectedPointAnnotationKind,
    #[serde(default)]
    pub online: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azimuth_deg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default)]
    pub style: ProjectedPointAnnotationStyle,
}

impl ProjectedPointAnnotation {
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            kind: ProjectedPointAnnotationKind::Camera,
            online: true,
            azimuth_deg: None,
            label: None,
            style: ProjectedPointAnnotationStyle::default(),
        }
    }

    pub fn with_kind(mut self, kind: ProjectedPointAnnotationKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_online(mut self, online: bool) -> Self {
        self.online = online;
        self
    }

    pub fn with_azimuth_deg(mut self, azimuth_deg: f64) -> Self {
        self.azimuth_deg = Some(azimuth_deg);
        self
    }

    pub fn with_label<S: Into<String>>(mut self, label: S) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn with_style(mut self, style: ProjectedPointAnnotationStyle) -> Self {
        self.style = style;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPoiCatalog {
    pub source_slug: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
    pub items: Vec<CustomPoi>,
}

impl CustomPoiCatalog {
    pub fn load_json(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn california_square() -> (f64, f64, f64, f64) {
        (-124.9, -113.7, 31.8, 42.7)
    }

    pub fn filtered_to_bounds(&self, bounds: (f64, f64, f64, f64)) -> Self {
        let (west, east, south, north) = bounds;
        let mut items = self
            .items
            .iter()
            .filter(|poi| {
                poi.lat_deg.is_finite()
                    && poi.lon_deg.is_finite()
                    && poi.lat_deg >= south
                    && poi.lat_deg <= north
                    && poi.lon_deg >= west
                    && poi.lon_deg <= east
            })
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| left.id.cmp(&right.id));
        Self {
            source_slug: self.source_slug.clone(),
            source_label: self.source_label.clone(),
            generated_at: self.generated_at.clone(),
            items,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPoi {
    pub id: String,
    pub name: String,
    #[serde(default = "default_camera_kind")]
    pub kind: String,
    pub lat_deg: f64,
    pub lon_deg: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading_deg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tilt_deg: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zoom_scale: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub county: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_online: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub camera_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_url: Option<String>,
}

impl CustomPoi {
    pub fn short_label(&self) -> String {
        let candidate = self
            .site_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(self.name.as_str());
        let mut label = candidate.replace('_', " ");
        if label.len() > 18 {
            label.truncate(18);
        }
        label
    }
}

fn default_camera_kind() -> String {
    "camera".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PoiClusterConfig {
    pub max_link_distance_km: f64,
    pub min_points: usize,
    pub domain_pad_fraction: f64,
    pub min_half_height_deg: f64,
    pub target_aspect_ratio: f64,
}

impl Default for PoiClusterConfig {
    fn default() -> Self {
        Self {
            max_link_distance_km: 18.0,
            min_points: 4,
            domain_pad_fraction: 0.28,
            min_half_height_deg: 0.16,
            target_aspect_ratio: DEFAULT_CAMERA_CLUSTER_ASPECT_RATIO,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoiClusterRoute {
    pub slug: String,
    pub label: String,
    pub start_lat_deg: f64,
    pub start_lon_deg: f64,
    pub end_lat_deg: f64,
    pub end_lon_deg: f64,
    pub distance_km: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoiCluster {
    pub slug: String,
    pub label: String,
    pub center_lat_deg: f64,
    pub center_lon_deg: f64,
    pub bounds: (f64, f64, f64, f64),
    pub dominant_heading_deg: Option<f64>,
    pub poi_ids: Vec<String>,
    pub poi_count: usize,
    pub route: PoiClusterRoute,
}

impl PoiCluster {
    pub fn domain_spec(&self) -> DomainSpec {
        DomainSpec::new(self.slug.clone(), self.bounds)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomPoiOverlay {
    pub catalog: CustomPoiCatalog,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<PoiCluster>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview_bounds: Option<(f64, f64, f64, f64)>,
}

impl CustomPoiOverlay {
    pub fn new(catalog: CustomPoiCatalog) -> Self {
        Self {
            catalog,
            clusters: Vec::new(),
            overview_bounds: None,
        }
    }

    pub fn with_clusters(mut self, clusters: Vec<PoiCluster>) -> Self {
        self.clusters = clusters;
        self
    }

    pub fn with_overview_bounds(mut self, bounds: (f64, f64, f64, f64)) -> Self {
        self.overview_bounds = Some(bounds);
        self
    }

    pub fn cluster_domains(&self) -> Vec<DomainSpec> {
        self.clusters.iter().map(PoiCluster::domain_spec).collect()
    }
}

pub fn cluster_custom_pois(
    catalog: &CustomPoiCatalog,
    config: PoiClusterConfig,
) -> Vec<PoiCluster> {
    let points = catalog.items.as_slice();
    let n = points.len();
    if n == 0 {
        return Vec::new();
    }

    let mut adjacency = vec![Vec::<usize>::new(); n];
    for left in 0..n {
        for right in (left + 1)..n {
            let distance_km = haversine_km(
                points[left].lat_deg,
                points[left].lon_deg,
                points[right].lat_deg,
                points[right].lon_deg,
            );
            if distance_km <= config.max_link_distance_km {
                adjacency[left].push(right);
                adjacency[right].push(left);
            }
        }
    }

    let mut visited = vec![false; n];
    let mut raw_clusters = Vec::<Vec<usize>>::new();
    for start in 0..n {
        if visited[start] {
            continue;
        }
        let mut stack = vec![start];
        let mut cluster = Vec::new();
        visited[start] = true;
        while let Some(index) = stack.pop() {
            cluster.push(index);
            for &next in &adjacency[index] {
                if !visited[next] {
                    visited[next] = true;
                    stack.push(next);
                }
            }
        }
        if cluster.len() >= config.min_points {
            raw_clusters.push(cluster);
        }
    }

    raw_clusters.sort_by(|left, right| right.len().cmp(&left.len()));
    raw_clusters
        .into_iter()
        .enumerate()
        .map(|(cluster_index, members)| build_cluster(points, &members, cluster_index, config))
        .collect()
}

fn build_cluster(
    points: &[CustomPoi],
    members: &[usize],
    cluster_index: usize,
    config: PoiClusterConfig,
) -> PoiCluster {
    let pois = members
        .iter()
        .map(|&index| &points[index])
        .collect::<Vec<_>>();
    let center_lat_deg = pois.iter().map(|poi| poi.lat_deg).sum::<f64>() / pois.len() as f64;
    let center_lon_deg = pois.iter().map(|poi| poi.lon_deg).sum::<f64>() / pois.len() as f64;
    let north = pois
        .iter()
        .map(|poi| poi.lat_deg)
        .fold(f64::NEG_INFINITY, f64::max);
    let south = pois
        .iter()
        .map(|poi| poi.lat_deg)
        .fold(f64::INFINITY, f64::min);
    let east = pois
        .iter()
        .map(|poi| poi.lon_deg)
        .fold(f64::NEG_INFINITY, f64::max);
    let west = pois
        .iter()
        .map(|poi| poi.lon_deg)
        .fold(f64::INFINITY, f64::min);
    let bounds = expand_bounds_to_aspect(
        (west, east, south, north),
        config.domain_pad_fraction,
        config.min_half_height_deg,
        config.target_aspect_ratio,
    );
    let label = cluster_label(pois.as_slice(), cluster_index);
    let slug = cluster_slug(pois.as_slice(), &label, cluster_index);
    let dominant_heading_deg = circular_mean_deg(
        &pois
            .iter()
            .filter_map(|poi| poi.heading_deg)
            .collect::<Vec<_>>(),
    );
    let route = build_cluster_route(
        &slug,
        &label,
        pois.as_slice(),
        center_lat_deg,
        center_lon_deg,
    );
    let mut poi_ids = pois.iter().map(|poi| poi.id.clone()).collect::<Vec<_>>();
    poi_ids.sort();
    PoiCluster {
        slug,
        label,
        center_lat_deg,
        center_lon_deg,
        bounds,
        dominant_heading_deg,
        poi_ids,
        poi_count: pois.len(),
        route,
    }
}

fn cluster_label(pois: &[&CustomPoi], cluster_index: usize) -> String {
    let mut county_counts = HashMap::<String, usize>::new();
    for poi in pois {
        if let Some(county) = poi.county.as_deref().filter(|value| !value.is_empty()) {
            *county_counts.entry(title_case(county)).or_default() += 1;
        }
    }
    if let Some((county, _)) = county_counts.into_iter().max_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0).reverse())
    }) {
        return format!("{county} Cameras");
    }
    let name = pois
        .first()
        .map(|poi| poi.short_label())
        .unwrap_or_else(|| format!("Cluster {:02}", cluster_index + 1));
    format!("{name} Area")
}

fn cluster_slug(pois: &[&CustomPoi], label: &str, cluster_index: usize) -> String {
    if pois.len() == 1 {
        return slugify(&pois[0].id);
    }
    let label_slug = slugify(label);
    if label_slug.is_empty() {
        format!("camera_cluster_{:02}", cluster_index + 1)
    } else {
        format!("camera_cluster_{:02}_{}", cluster_index + 1, label_slug)
    }
}

fn build_cluster_route(
    slug: &str,
    label: &str,
    pois: &[&CustomPoi],
    center_lat_deg: f64,
    center_lon_deg: f64,
) -> PoiClusterRoute {
    let cos_lat = center_lat_deg.to_radians().cos().abs().max(0.25);
    let mut xx = 0.0f64;
    let mut xy = 0.0f64;
    let mut yy = 0.0f64;
    let mut max_abs_proj = 0.0f64;
    let mut max_abs_cross = 0.0f64;

    let local_points = pois
        .iter()
        .map(|poi| {
            let x = (poi.lon_deg - center_lon_deg) * KM_PER_DEG_LAT * cos_lat;
            let y = (poi.lat_deg - center_lat_deg) * KM_PER_DEG_LAT;
            xx += x * x;
            xy += x * y;
            yy += y * y;
            (x, y)
        })
        .collect::<Vec<_>>();

    let angle = 0.5 * (2.0 * xy).atan2(xx - yy);
    let axis_x = angle.cos();
    let axis_y = angle.sin();
    let cross_x = -axis_y;
    let cross_y = axis_x;

    for &(x, y) in &local_points {
        let along = x * axis_x + y * axis_y;
        let across = x * cross_x + y * cross_y;
        max_abs_proj = max_abs_proj.max(along.abs());
        max_abs_cross = max_abs_cross.max(across.abs());
    }

    let half_length_km = (max_abs_proj + max_abs_cross * 0.75 + 12.0).max(65.0);
    let start_lat_deg = center_lat_deg - axis_y * half_length_km / KM_PER_DEG_LAT;
    let end_lat_deg = center_lat_deg + axis_y * half_length_km / KM_PER_DEG_LAT;
    let start_lon_deg = center_lon_deg - axis_x * half_length_km / (KM_PER_DEG_LAT * cos_lat);
    let end_lon_deg = center_lon_deg + axis_x * half_length_km / (KM_PER_DEG_LAT * cos_lat);
    PoiClusterRoute {
        slug: format!("{slug}_section"),
        label: format!("{label} Section"),
        start_lat_deg,
        start_lon_deg,
        end_lat_deg,
        end_lon_deg,
        distance_km: haversine_km(start_lat_deg, start_lon_deg, end_lat_deg, end_lon_deg),
    }
}

pub fn build_projected_camera_annotations(
    pois: &[CustomPoi],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&GridProjection>,
) -> Result<Vec<ProjectedPointAnnotation>, Box<dyn std::error::Error>> {
    let projected = project_geographic_points(
        &pois
            .iter()
            .map(|poi| (poi.lat_deg, poi.lon_deg))
            .collect::<Vec<_>>(),
        grid_lat_deg,
        grid_lon_deg,
        projection,
    )?;
    Ok(pois
        .iter()
        .zip(projected.into_iter())
        .map(|(poi, (x, y))| {
            let style = camera_annotation_style(poi);
            let mut annotation = ProjectedPointAnnotation::new(x, y)
                .with_kind(ProjectedPointAnnotationKind::Camera)
                .with_online(poi.is_online.unwrap_or(true))
                .with_label(poi.short_label())
                .with_style(style);
            if let Some(heading_deg) = poi.heading_deg {
                annotation = annotation.with_azimuth_deg(heading_deg);
            }
            annotation
        })
        .collect())
}

pub fn build_projected_camera_annotations_for_projected_grid(
    pois: &[CustomPoi],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projected_x: &[f64],
    projected_y: &[f64],
) -> Result<Vec<ProjectedPointAnnotation>, Box<dyn std::error::Error>> {
    let projected = interpolate_projected_points(
        &pois
            .iter()
            .map(|poi| (poi.lat_deg, poi.lon_deg))
            .collect::<Vec<_>>(),
        grid_lat_deg,
        grid_lon_deg,
        projected_x,
        projected_y,
    )?;
    Ok(pois
        .iter()
        .zip(projected.into_iter())
        .map(|(poi, (x, y))| {
            let style = camera_annotation_style(poi);
            let mut annotation = ProjectedPointAnnotation::new(x, y)
                .with_kind(ProjectedPointAnnotationKind::Camera)
                .with_online(poi.is_online.unwrap_or(true))
                .with_label(poi.short_label())
                .with_style(style);
            if let Some(heading_deg) = poi.heading_deg {
                annotation = annotation.with_azimuth_deg(heading_deg);
            }
            annotation
        })
        .collect())
}

pub fn build_projected_annotation_line_overlays(
    annotations: &[ProjectedPointAnnotation],
    projected_domain: &rustwx_render::ProjectedDomain,
    width: u32,
    height: u32,
) -> Vec<ProjectedLineOverlay> {
    let units_per_px = projected_units_per_pixel(projected_domain, width, height);
    annotations
        .iter()
        .flat_map(|annotation| annotation_line_overlays(annotation, units_per_px))
        .collect()
}

pub fn apply_custom_poi_overlay(
    request: &mut MapRenderRequest,
    overlay: &CustomPoiOverlay,
    domain_bounds: (f64, f64, f64, f64),
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&GridProjection>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(projected_domain) = request.projected_domain.as_ref() else {
        return Ok(());
    };
    let units_per_px = projected_units_per_pixel(projected_domain, request.width, request.height);
    let visible_catalog = overlay.catalog.filtered_to_bounds(domain_bounds);
    if !visible_catalog.items.is_empty() {
        let annotations = build_projected_camera_annotations(
            &visible_catalog.items,
            grid_lat_deg,
            grid_lon_deg,
            projection,
        )?;
        request.projected_lines.extend(
            annotations
                .iter()
                .flat_map(|annotation| annotation_line_overlays(annotation, units_per_px))
                .collect::<Vec<_>>(),
        );
    }

    if overlay
        .overview_bounds
        .map(|bounds| bounds_almost_equal(bounds, domain_bounds))
        .unwrap_or(false)
    {
        let visible_clusters = overlay
            .clusters
            .iter()
            .filter(|cluster| bounds_intersect(cluster.bounds, domain_bounds))
            .cloned()
            .collect::<Vec<_>>();
        if !visible_clusters.is_empty() {
            let (lines, annotations) = build_projected_cluster_overview_geometry(
                &visible_clusters,
                grid_lat_deg,
                grid_lon_deg,
                projection,
            )?;
            request.projected_lines.extend(lines);
            request.projected_lines.extend(
                annotations
                    .iter()
                    .flat_map(|annotation| annotation_line_overlays(annotation, units_per_px))
                    .collect::<Vec<_>>(),
            );
        }
    }

    Ok(())
}

pub fn build_projected_cluster_overview_geometry(
    clusters: &[PoiCluster],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&GridProjection>,
) -> Result<(Vec<ProjectedLineOverlay>, Vec<ProjectedPointAnnotation>), Box<dyn std::error::Error>>
{
    let mut rectangle_vertices = Vec::<(f64, f64)>::new();
    let mut rectangle_sizes = Vec::<usize>::new();
    let mut cluster_centers = Vec::<(f64, f64)>::new();
    for cluster in clusters {
        let (west, east, south, north) = cluster.bounds;
        let ring = [
            (north, west),
            (north, east),
            (south, east),
            (south, west),
            (north, west),
        ];
        rectangle_vertices.extend(ring);
        rectangle_sizes.push(ring.len());
        cluster_centers.push((cluster.center_lat_deg, cluster.center_lon_deg));
    }
    let projected_rectangles =
        project_geographic_points(&rectangle_vertices, grid_lat_deg, grid_lon_deg, projection)?;
    let projected_centers =
        project_geographic_points(&cluster_centers, grid_lat_deg, grid_lon_deg, projection)?;

    let mut lines = Vec::with_capacity(clusters.len());
    let mut cursor = 0usize;
    for size in rectangle_sizes {
        let points = projected_rectangles[cursor..cursor + size].to_vec();
        cursor += size;
        lines.push(ProjectedLineOverlay {
            points,
            color: Color::rgba(255, 111, 0, 255),
            width: 3,
            role: LineworkRole::Generic,
        });
    }

    let annotations = clusters
        .iter()
        .zip(projected_centers.into_iter())
        .map(|(cluster, (x, y))| {
            ProjectedPointAnnotation::new(x, y)
                .with_kind(ProjectedPointAnnotationKind::Cluster)
                .with_label(format!("{} ({})", cluster.label, cluster.poi_count))
                .with_style(cluster_label_style())
        })
        .collect();
    Ok((lines, annotations))
}

pub fn build_projected_cluster_overview_geometry_for_projected_grid(
    clusters: &[PoiCluster],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projected_x: &[f64],
    projected_y: &[f64],
) -> Result<(Vec<ProjectedLineOverlay>, Vec<ProjectedPointAnnotation>), Box<dyn std::error::Error>>
{
    let mut rectangle_vertices = Vec::<(f64, f64)>::new();
    let mut rectangle_sizes = Vec::<usize>::new();
    let mut cluster_centers = Vec::<(f64, f64)>::new();
    for cluster in clusters {
        let (west, east, south, north) = cluster.bounds;
        let ring = [
            (north, west),
            (north, east),
            (south, east),
            (south, west),
            (north, west),
        ];
        rectangle_vertices.extend(ring);
        rectangle_sizes.push(ring.len());
        cluster_centers.push((cluster.center_lat_deg, cluster.center_lon_deg));
    }
    let projected_rectangles = interpolate_projected_points(
        &rectangle_vertices,
        grid_lat_deg,
        grid_lon_deg,
        projected_x,
        projected_y,
    )?;
    let projected_centers = interpolate_projected_points(
        &cluster_centers,
        grid_lat_deg,
        grid_lon_deg,
        projected_x,
        projected_y,
    )?;

    let mut lines = Vec::with_capacity(clusters.len());
    let mut cursor = 0usize;
    for size in rectangle_sizes {
        let points = projected_rectangles[cursor..cursor + size].to_vec();
        cursor += size;
        lines.push(ProjectedLineOverlay {
            points,
            color: Color::rgba(255, 111, 0, 255),
            width: 3,
            role: LineworkRole::Generic,
        });
    }

    let annotations = clusters
        .iter()
        .zip(projected_centers.into_iter())
        .map(|(cluster, (x, y))| {
            ProjectedPointAnnotation::new(x, y)
                .with_kind(ProjectedPointAnnotationKind::Cluster)
                .with_label(format!("{} ({})", cluster.label, cluster.poi_count))
                .with_style(cluster_label_style())
        })
        .collect();
    Ok((lines, annotations))
}

pub fn build_projected_cluster_route_line_overlays_for_projected_grid(
    route: &PoiClusterRoute,
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projected_x: &[f64],
    projected_y: &[f64],
) -> Result<Vec<ProjectedLineOverlay>, Box<dyn std::error::Error>> {
    let points = interpolate_projected_points(
        &[
            (route.start_lat_deg, route.start_lon_deg),
            (route.end_lat_deg, route.end_lon_deg),
        ],
        grid_lat_deg,
        grid_lon_deg,
        projected_x,
        projected_y,
    )?;
    Ok(vec![
        ProjectedLineOverlay {
            points: points.clone(),
            color: Color::rgba(0, 0, 0, 255),
            width: 7,
            role: LineworkRole::Generic,
        },
        ProjectedLineOverlay {
            points,
            color: Color::rgba(64, 240, 255, 255),
            width: 4,
            role: LineworkRole::Generic,
        },
    ])
}

pub fn project_geographic_points(
    geographic_points: &[(f64, f64)],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&GridProjection>,
) -> Result<Vec<(f64, f64)>, Box<dyn std::error::Error>> {
    if geographic_points.is_empty() {
        return Ok(Vec::new());
    }
    let lat = geographic_points
        .iter()
        .map(|&(lat_deg, _)| lat_deg as f32)
        .collect::<Vec<_>>();
    let lon = geographic_points
        .iter()
        .map(|&(_, lon_deg)| lon_deg as f32)
        .collect::<Vec<_>>();

    let mut options = ProjectedDomainBuildOptions::full_domain(1.0);
    if let Some(reference_latitude_deg) = latitude_midpoint_deg(grid_lat_deg) {
        options = options.with_reference_latitude(reference_latitude_deg);
    }
    if let Some(projection_spec) = resolve_projection_spec(grid_lat_deg, grid_lon_deg, projection) {
        options = options.with_projection(projection_spec);
    }
    let projected = build_projected_domain(&lat, &lon, &options)?;
    Ok(projected.x.into_iter().zip(projected.y).collect())
}

fn interpolate_projected_points(
    geographic_points: &[(f64, f64)],
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projected_x: &[f64],
    projected_y: &[f64],
) -> Result<Vec<(f64, f64)>, Box<dyn std::error::Error>> {
    let expected = grid_lat_deg.len();
    if grid_lon_deg.len() != expected
        || projected_x.len() != expected
        || projected_y.len() != expected
    {
        return Err(
            "camera projection interpolation requires matching grid/projected lengths".into(),
        );
    }
    Ok(geographic_points
        .iter()
        .map(|&(lat_deg, lon_deg)| {
            interpolate_projected_point(
                lat_deg,
                lon_deg,
                grid_lat_deg,
                grid_lon_deg,
                projected_x,
                projected_y,
            )
        })
        .collect())
}

fn interpolate_projected_point(
    lat_deg: f64,
    lon_deg: f64,
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projected_x: &[f64],
    projected_y: &[f64],
) -> (f64, f64) {
    const KEEP: usize = 4;
    let cos_lat = lat_deg.to_radians().cos().abs().max(0.25);
    let mut nearest = [(usize::MAX, f64::INFINITY); KEEP];
    let mut nearest_len = 0usize;
    for (index, (&grid_lat, &grid_lon)) in grid_lat_deg.iter().zip(grid_lon_deg.iter()).enumerate()
    {
        let dlat = grid_lat as f64 - lat_deg;
        let dlon = (grid_lon as f64 - lon_deg) * cos_lat;
        let distance = dlat * dlat + dlon * dlon;
        insert_best_projected_candidate(&mut nearest, &mut nearest_len, index, distance);
    }
    if nearest[0].1 <= 1.0e-12 {
        return (projected_x[nearest[0].0], projected_y[nearest[0].0]);
    }

    let mut weight_sum = 0.0;
    let mut x_sum = 0.0;
    let mut y_sum = 0.0;
    for &(index, distance) in nearest.iter().take(nearest_len.max(1)) {
        if index == usize::MAX {
            continue;
        }
        let weight = 1.0 / distance.max(1.0e-12);
        weight_sum += weight;
        x_sum += projected_x[index] * weight;
        y_sum += projected_y[index] * weight;
    }
    if weight_sum <= 1.0e-12 {
        (projected_x[nearest[0].0], projected_y[nearest[0].0])
    } else {
        (x_sum / weight_sum, y_sum / weight_sum)
    }
}

fn insert_best_projected_candidate(
    nearest: &mut [(usize, f64); 4],
    nearest_len: &mut usize,
    index: usize,
    distance: f64,
) {
    let keep = nearest.len();
    let mut insert_at = (*nearest_len).min(keep);
    while insert_at > 0
        && (distance < nearest[insert_at - 1].1
            || ((distance - nearest[insert_at - 1].1).abs() <= 1.0e-12
                && index < nearest[insert_at - 1].0))
    {
        insert_at -= 1;
    }
    if insert_at >= keep {
        return;
    }
    if *nearest_len < keep {
        *nearest_len += 1;
    }
    for slot in (insert_at + 1..*nearest_len).rev() {
        nearest[slot] = nearest[slot - 1];
    }
    nearest[insert_at] = (index, distance);
}

fn resolve_projection_spec(
    grid_lat_deg: &[f32],
    grid_lon_deg: &[f32],
    projection: Option<&GridProjection>,
) -> Option<ProjectionSpec> {
    projection
        .cloned()
        .map(Into::into)
        .or_else(|| ProjectionSpec::infer_from_latlon_grid(grid_lat_deg, grid_lon_deg))
}

fn camera_annotation_style(poi: &CustomPoi) -> ProjectedPointAnnotationStyle {
    let online = poi.is_online.unwrap_or(true);
    ProjectedPointAnnotationStyle {
        marker_radius_px: 5.0,
        marker_fill: if online {
            Color::rgba(255, 196, 61, 255)
        } else {
            Color::rgba(166, 166, 166, 255)
        },
        marker_outline: Color::BLACK,
        marker_outline_width: 2,
        arrow_color: if online {
            Color::rgba(102, 17, 0, 255)
        } else {
            Color::rgba(90, 90, 90, 255)
        },
        arrow_length_px: 26.0,
        arrow_width: 2,
        label_color: Color::BLACK,
        label_halo: Color::WHITE,
        label_offset_x_px: 10,
        label_offset_y_px: -14,
        label_scale: 1,
    }
}

fn cluster_label_style() -> ProjectedPointAnnotationStyle {
    ProjectedPointAnnotationStyle {
        marker_radius_px: 4.0,
        marker_fill: Color::rgba(255, 111, 0, 235),
        marker_outline: Color::WHITE,
        marker_outline_width: 2,
        arrow_color: Color::TRANSPARENT,
        arrow_length_px: 0.0,
        arrow_width: 0,
        label_color: Color::rgba(121, 32, 0, 255),
        label_halo: Color::WHITE,
        label_offset_x_px: 10,
        label_offset_y_px: -18,
        label_scale: 2,
    }
}

fn projected_units_per_pixel(
    projected_domain: &rustwx_render::ProjectedDomain,
    width: u32,
    height: u32,
) -> f64 {
    let width_units =
        (projected_domain.extent.x_max - projected_domain.extent.x_min).abs() / width.max(1) as f64;
    let height_units = (projected_domain.extent.y_max - projected_domain.extent.y_min).abs()
        / height.max(1) as f64;
    width_units.max(height_units).max(1.0e-9)
}

fn annotation_line_overlays(
    annotation: &ProjectedPointAnnotation,
    units_per_px: f64,
) -> Vec<ProjectedLineOverlay> {
    let radius = (annotation.style.marker_radius_px * units_per_px).max(units_per_px * 2.0);
    let color = match annotation.kind {
        ProjectedPointAnnotationKind::Camera => annotation.style.marker_outline,
        ProjectedPointAnnotationKind::Cluster => annotation.style.marker_fill,
    };
    let width = annotation.style.marker_outline_width.max(1);
    let mut overlays = vec![
        ProjectedLineOverlay {
            points: vec![
                (annotation.x - radius, annotation.y),
                (annotation.x + radius, annotation.y),
            ],
            color,
            width,
            role: LineworkRole::Generic,
        },
        ProjectedLineOverlay {
            points: vec![
                (annotation.x, annotation.y - radius),
                (annotation.x, annotation.y + radius),
            ],
            color,
            width,
            role: LineworkRole::Generic,
        },
    ];

    if matches!(annotation.kind, ProjectedPointAnnotationKind::Cluster) {
        overlays.push(ProjectedLineOverlay {
            points: vec![
                (annotation.x - radius, annotation.y - radius),
                (annotation.x + radius, annotation.y + radius),
            ],
            color,
            width,
            role: LineworkRole::Generic,
        });
        overlays.push(ProjectedLineOverlay {
            points: vec![
                (annotation.x - radius, annotation.y + radius),
                (annotation.x + radius, annotation.y - radius),
            ],
            color,
            width,
            role: LineworkRole::Generic,
        });
    }

    if let Some(azimuth_deg) = annotation.azimuth_deg.filter(|value| value.is_finite()) {
        let azimuth_rad = azimuth_deg.to_radians();
        let shaft_length = (annotation.style.arrow_length_px * units_per_px).max(radius * 1.8);
        let dx = shaft_length * azimuth_rad.sin();
        let dy = shaft_length * azimuth_rad.cos();
        let tip = (annotation.x + dx, annotation.y + dy);
        let head_length = (shaft_length * 0.28).max(radius * 0.95);
        let head_angle = 26.0_f64.to_radians();
        let back_left = (
            tip.0 + head_length * (azimuth_rad + std::f64::consts::PI - head_angle).sin(),
            tip.1 + head_length * (azimuth_rad + std::f64::consts::PI - head_angle).cos(),
        );
        let back_right = (
            tip.0 + head_length * (azimuth_rad + std::f64::consts::PI + head_angle).sin(),
            tip.1 + head_length * (azimuth_rad + std::f64::consts::PI + head_angle).cos(),
        );
        overlays.push(ProjectedLineOverlay {
            points: vec![(annotation.x, annotation.y), tip],
            color: annotation.style.arrow_color,
            width: annotation.style.arrow_width.max(1),
            role: LineworkRole::Generic,
        });
        overlays.push(ProjectedLineOverlay {
            points: vec![tip, back_left],
            color: annotation.style.arrow_color,
            width: annotation.style.arrow_width.max(1),
            role: LineworkRole::Generic,
        });
        overlays.push(ProjectedLineOverlay {
            points: vec![tip, back_right],
            color: annotation.style.arrow_color,
            width: annotation.style.arrow_width.max(1),
            role: LineworkRole::Generic,
        });
    }

    overlays
}

fn latitude_midpoint_deg(values: &[f32]) -> Option<f64> {
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    for &value in values {
        let value = value as f64;
        if !value.is_finite() {
            continue;
        }
        min_lat = min_lat.min(value);
        max_lat = max_lat.max(value);
    }
    if min_lat.is_finite() && max_lat.is_finite() {
        Some((min_lat + max_lat) * 0.5)
    } else {
        None
    }
}

fn expand_bounds_to_aspect(
    bounds: (f64, f64, f64, f64),
    pad_fraction: f64,
    min_half_height_deg: f64,
    aspect_ratio: f64,
) -> (f64, f64, f64, f64) {
    let (west, east, south, north) = bounds;
    let center_lon = (west + east) * 0.5;
    let center_lat = (south + north) * 0.5;
    let cos_lat = center_lat.to_radians().cos().abs().max(0.25);
    let raw_half_height = ((north - south) * 0.5).max(min_half_height_deg);
    let raw_half_width_km = ((east - west) * 0.5) * KM_PER_DEG_LAT * cos_lat;
    let raw_half_height_km = raw_half_height * KM_PER_DEG_LAT;
    let half_height_km = raw_half_height_km.max(raw_half_width_km / aspect_ratio.max(0.25))
        * (1.0 + pad_fraction.max(0.0));
    let half_height_deg = (half_height_km / KM_PER_DEG_LAT).max(min_half_height_deg);
    let half_width_deg = half_height_deg * aspect_ratio.max(0.25) / cos_lat;
    (
        center_lon - half_width_deg,
        center_lon + half_width_deg,
        center_lat - half_height_deg,
        center_lat + half_height_deg,
    )
}

fn haversine_km(lat0_deg: f64, lon0_deg: f64, lat1_deg: f64, lon1_deg: f64) -> f64 {
    let lat0 = lat0_deg.to_radians();
    let lat1 = lat1_deg.to_radians();
    let dlat = (lat1_deg - lat0_deg).to_radians();
    let dlon = (lon1_deg - lon0_deg).to_radians();
    let a = (dlat * 0.5).sin().powi(2) + lat0.cos() * lat1.cos() * (dlon * 0.5).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    6371.0 * c
}

fn circular_mean_deg(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let sin_sum = values
        .iter()
        .map(|value| value.to_radians().sin())
        .sum::<f64>();
    let cos_sum = values
        .iter()
        .map(|value| value.to_radians().cos())
        .sum::<f64>();
    if sin_sum.abs() < 1.0e-9 && cos_sum.abs() < 1.0e-9 {
        None
    } else {
        Some(sin_sum.atan2(cos_sum).to_degrees().rem_euclid(360.0))
    }
}

fn title_case(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    let mut out = first.to_uppercase().collect::<String>();
                    out.push_str(chars.as_str());
                    out
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn slugify(value: &str) -> String {
    let mut slug = String::with_capacity(value.len());
    let mut last_was_separator = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_was_separator = false;
        } else if !slug.is_empty() && !last_was_separator {
            slug.push('_');
            last_was_separator = true;
        }
    }
    slug.trim_matches('_').to_string()
}

fn bounds_intersect(left: (f64, f64, f64, f64), right: (f64, f64, f64, f64)) -> bool {
    let (left_west, left_east, left_south, left_north) = left;
    let (right_west, right_east, right_south, right_north) = right;
    left_west <= right_east
        && left_east >= right_west
        && left_south <= right_north
        && left_north >= right_south
}

fn bounds_almost_equal(left: (f64, f64, f64, f64), right: (f64, f64, f64, f64)) -> bool {
    let epsilon = 1.0e-6;
    (left.0 - right.0).abs() <= epsilon
        && (left.1 - right.1).abs() <= epsilon
        && (left.2 - right.2).abs() <= epsilon
        && (left.3 - right.3).abs() <= epsilon
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_catalog() -> CustomPoiCatalog {
        CustomPoiCatalog {
            source_slug: "alertcalifornia".to_string(),
            source_label: Some("AlertCalifornia".to_string()),
            generated_at: None,
            items: vec![
                CustomPoi {
                    id: "a".to_string(),
                    name: "Alpha".to_string(),
                    kind: "camera".to_string(),
                    lat_deg: 38.5,
                    lon_deg: -121.5,
                    heading_deg: Some(90.0),
                    tilt_deg: None,
                    zoom_scale: None,
                    view_time: None,
                    site_id: Some("alpha".to_string()),
                    county: Some("sacramento".to_string()),
                    state: Some("CA".to_string()),
                    is_online: Some(true),
                    camera_url: None,
                    network_url: None,
                    image_url: None,
                },
                CustomPoi {
                    id: "b".to_string(),
                    name: "Beta".to_string(),
                    kind: "camera".to_string(),
                    lat_deg: 38.55,
                    lon_deg: -121.45,
                    heading_deg: Some(95.0),
                    tilt_deg: None,
                    zoom_scale: None,
                    view_time: None,
                    site_id: Some("beta".to_string()),
                    county: Some("sacramento".to_string()),
                    state: Some("CA".to_string()),
                    is_online: Some(true),
                    camera_url: None,
                    network_url: None,
                    image_url: None,
                },
                CustomPoi {
                    id: "c".to_string(),
                    name: "Gamma".to_string(),
                    kind: "camera".to_string(),
                    lat_deg: 38.58,
                    lon_deg: -121.48,
                    heading_deg: Some(110.0),
                    tilt_deg: None,
                    zoom_scale: None,
                    view_time: None,
                    site_id: Some("gamma".to_string()),
                    county: Some("sacramento".to_string()),
                    state: Some("CA".to_string()),
                    is_online: Some(true),
                    camera_url: None,
                    network_url: None,
                    image_url: None,
                },
                CustomPoi {
                    id: "d".to_string(),
                    name: "Delta".to_string(),
                    kind: "camera".to_string(),
                    lat_deg: 38.53,
                    lon_deg: -121.42,
                    heading_deg: Some(100.0),
                    tilt_deg: None,
                    zoom_scale: None,
                    view_time: None,
                    site_id: Some("delta".to_string()),
                    county: Some("sacramento".to_string()),
                    state: Some("CA".to_string()),
                    is_online: Some(true),
                    camera_url: None,
                    network_url: None,
                    image_url: None,
                },
            ],
        }
    }

    #[test]
    fn clustering_groups_nearby_camera_points() {
        let clusters = cluster_custom_pois(&sample_catalog(), PoiClusterConfig::default());
        assert_eq!(clusters.len(), 1);
        let cluster = &clusters[0];
        assert_eq!(cluster.poi_count, 4);
        assert_eq!(cluster.slug, "camera_cluster_01_sacramento_cameras");
        assert!(cluster.bounds.0 < cluster.bounds.1);
        assert!(cluster.route.distance_km > 50.0);
    }

    #[test]
    fn california_filter_drops_points_outside_bounds() {
        let mut catalog = sample_catalog();
        catalog.items.push(CustomPoi {
            id: "z".to_string(),
            name: "Outside".to_string(),
            kind: "camera".to_string(),
            lat_deg: 25.0,
            lon_deg: -100.0,
            heading_deg: None,
            tilt_deg: None,
            zoom_scale: None,
            view_time: None,
            site_id: None,
            county: None,
            state: None,
            is_online: None,
            camera_url: None,
            network_url: None,
            image_url: None,
        });
        let filtered = catalog.filtered_to_bounds(CustomPoiCatalog::california_square());
        assert_eq!(filtered.items.len(), 4);
    }

    #[test]
    fn projected_camera_annotations_preserve_heading_and_labels() {
        let catalog = sample_catalog();
        let lat = vec![38.0f32, 38.0, 39.0, 39.0];
        let lon = vec![-122.0f32, -121.0, -122.0, -121.0];
        let projected = build_projected_camera_annotations(&catalog.items, &lat, &lon, None)
            .expect("projection should succeed");
        assert_eq!(projected.len(), catalog.items.len());
        assert_eq!(projected[0].azimuth_deg, Some(90.0));
        assert!(
            projected[0]
                .label
                .as_deref()
                .unwrap_or_default()
                .contains("alpha")
        );
    }

    #[test]
    fn apply_overlay_adds_visible_pois_and_cluster_overview() {
        let catalog = sample_catalog();
        let clusters = cluster_custom_pois(&catalog, PoiClusterConfig::default());
        let overlay = CustomPoiOverlay::new(catalog.clone())
            .with_clusters(clusters.clone())
            .with_overview_bounds(CustomPoiCatalog::california_square());
        let grid = rustwx_render::LatLonGrid::new(
            rustwx_render::GridShape::new(2, 2).unwrap(),
            vec![38.0, 38.0, 39.0, 39.0],
            vec![-122.0, -121.0, -122.0, -121.0],
        )
        .unwrap();
        let field = rustwx_render::Field2D::new(
            rustwx_render::ProductKey::named("custom_poi_overlay_test"),
            "unitless",
            grid,
            vec![0.0, 0.0, 0.0, 0.0],
        )
        .unwrap();
        let mut request = MapRenderRequest::contour_only(field);
        request.projected_domain = Some(rustwx_render::ProjectedDomain {
            x: vec![0.0, 1.0, 0.0, 1.0],
            y: vec![0.0, 0.0, 1.0, 1.0],
            extent: rustwx_render::ProjectedExtent {
                x_min: 0.0,
                x_max: 1.0,
                y_min: 0.0,
                y_max: 1.0,
            },
        });
        apply_custom_poi_overlay(
            &mut request,
            &overlay,
            CustomPoiCatalog::california_square(),
            &[38.0, 38.0, 39.0, 39.0],
            &[-122.0, -121.0, -122.0, -121.0],
            None,
        )
        .expect("overlay should project");

        assert!(
            request.projected_lines.len() >= (catalog.items.len() * 2) + 1,
            "overlay should add camera marker linework plus at least one cluster box"
        );
    }

    #[test]
    fn overlay_cluster_domains_follow_cluster_order() {
        let catalog = sample_catalog();
        let overlay = CustomPoiOverlay::new(catalog.clone())
            .with_clusters(cluster_custom_pois(&catalog, PoiClusterConfig::default()));

        let domains = overlay.cluster_domains();

        assert_eq!(domains.len(), 1);
        assert_eq!(domains[0].slug, "camera_cluster_01_sacramento_cameras");
    }
}
