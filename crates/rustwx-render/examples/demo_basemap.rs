//! Render-only preview that exercises the full basemap stack the way production
//! maps should: ocean fill + land fill + coastlines/borders, with masked CAPE
//! data painted on top so the basemap shows through wherever data is zero.
//!
//! Local to the render crates — no product/planner code imported.
use rustwx_render::{
    BasemapStyle, Color, ColorScale, ContourStyle, DiscreteColorScale, DomainFrame, ExtendMode,
    Field2D, GridShape, LambertConformal, LatLonGrid, MapRenderRequest, ProductKey,
    ProjectedDomain, ProjectedExtent, ProjectedLineOverlay, ProjectedPolygonFill,
    StyledLonLatLayer, StyledLonLatPolygonLayer, load_styled_conus_features_for,
    load_styled_conus_polygons_for, save_png,
};
use std::path::PathBuf;

// CONUS-centered Lambert Conformal Conic — matches HRRR-ish defaults.
const TRUE_LAT_1: f64 = 33.0;
const TRUE_LAT_2: f64 = 45.0;
const STAND_LON: f64 = -98.0;
const REF_LAT: f64 = 38.5;

// Tight CONUS-proper extent: lat 23-50 N, lon -124 to -66 W. Keeps the frame
// centered on the Lower 48 + near-shore oceans, matching the reference
// weathermodels.com crops.
const FRAME_LAT_MIN: f64 = 23.0;
const FRAME_LAT_MAX: f64 = 50.0;
const FRAME_LON_MIN: f64 = -124.0;
const FRAME_LON_MAX: f64 = -66.0;

const NX: usize = 540;
const NY: usize = 320;

fn main() {
    let proj = LambertConformal::new(TRUE_LAT_1, TRUE_LAT_2, STAND_LON, REF_LAT);

    let (x_sw, y_sw) = proj.project(FRAME_LAT_MIN, FRAME_LON_MIN);
    let (x_nw, y_nw) = proj.project(FRAME_LAT_MAX, FRAME_LON_MIN);
    let (x_se, y_se) = proj.project(FRAME_LAT_MIN, FRAME_LON_MAX);
    let (x_ne, y_ne) = proj.project(FRAME_LAT_MAX, FRAME_LON_MAX);
    let x_min = x_sw.min(x_nw);
    let x_max = x_se.max(x_ne);
    let y_min = y_sw.min(y_se);
    let y_max = y_nw.max(y_ne);

    let extent = ProjectedExtent {
        x_min,
        x_max,
        y_min,
        y_max,
    };

    let shape = GridShape::new(NX, NY).expect("valid grid");
    let len = shape.len();
    let mut lat = Vec::with_capacity(len);
    let mut lon = Vec::with_capacity(len);
    let mut proj_x = Vec::with_capacity(len);
    let mut proj_y = Vec::with_capacity(len);
    let mut values = Vec::with_capacity(len);
    let mut height_values = Vec::with_capacity(len);

    for j in 0..NY {
        let fy = j as f64 / (NY - 1) as f64;
        let y = y_min + fy * (y_max - y_min);
        for i in 0..NX {
            let fx = i as f64 / (NX - 1) as f64;
            let x = x_min + fx * (x_max - x_min);
            let (approx_lat, approx_lon) = inverse_lambert(&proj, x, y);

            // Synthetic CAPE-like field confined to the plains / southern US.
            // Values outside the blob stay below 250 so mask_below drops them
            // to fully transparent, letting the land fill show through.
            let dx = (approx_lon - -97.0) / 8.0;
            let dy = (approx_lat - 36.0) / 5.0;
            let blob = (-((dx * dx) + (dy * dy))).exp() * 4200.0;
            let tongue = (-(((approx_lon - -89.0) / 6.0).powi(2)
                + ((approx_lat - 32.5) / 3.0).powi(2)))
            .exp()
                * 3000.0;
            let cape = (blob + tongue).max(0.0);
            values.push(cape as f32);

            // Synthetic 500 mb height surrogate for contour overlay (m).
            let lat_comp = 5820.0 - ((approx_lat - 37.5).abs() * 8.0);
            let wave = ((approx_lon + 98.0) * 0.10).sin() * 35.0;
            let trough = (-(((approx_lon - -108.0) / 12.0).powi(2)
                + ((approx_lat - 41.0) / 7.0).powi(2)))
            .exp()
                * -90.0;
            let ridge = (-(((approx_lon - -82.0) / 12.0).powi(2)
                + ((approx_lat - 36.0) / 7.0).powi(2)))
            .exp()
                * 60.0;
            let height = lat_comp + wave + trough + ridge;
            height_values.push(height as f32);

            lat.push(approx_lat as f32);
            lon.push(approx_lon as f32);
            proj_x.push(x);
            proj_y.push(y);
        }
    }

    let grid = LatLonGrid::new(shape, lat, lon).expect("grid");
    let field =
        Field2D::new(ProductKey::named("SBECAPE"), "J/kg", grid.clone(), values).expect("field");
    let height_field =
        Field2D::new(ProductKey::named("HEIGHT"), "m", grid, height_values).expect("height field");

    let proof_dir = workspace_proof_dir();
    std::fs::create_dir_all(&proof_dir).expect("proof dir");

    for (style, filename) in [
        (BasemapStyle::Filled, "rustwx_render_demo_basemap.png"),
        (BasemapStyle::White, "rustwx_render_demo_basemap_white.png"),
    ] {
        let mut request = MapRenderRequest::new(field.clone(), cape_scale_masked());
        request.title = Some(match style {
            BasemapStyle::Filled => "SBECAPE — filled basemap".to_string(),
            BasemapStyle::White => "SBECAPE — white basemap (NWS-style)".to_string(),
        });
        request.subtitle_left = Some("Synthetic field · Lambert Conformal CONUS".to_string());
        request.subtitle_right = Some("rustwx-render native engine".to_string());
        request.cbar_tick_step = Some(500.0);
        request.domain_frame = Some(DomainFrame::model_data_default());
        request.projected_domain = Some(ProjectedDomain {
            x: proj_x.clone(),
            y: proj_y.clone(),
            extent: extent.clone(),
        });
        request.projected_polygons = project_polygons(&proj, &extent, style);
        request.projected_lines = project_lines(&proj, &extent, style);
        request = request
            .with_contour_field(
                &height_field,
                (5640..=5880).step_by(30).map(|h| h as f64).collect(),
                ContourStyle {
                    color: Color::rgba(30, 34, 44, 200),
                    width: 1,
                    labels: true,
                    show_extrema: true,
                },
            )
            .expect("contour");

        let output = proof_dir.join(filename);
        save_png(&request, &output).expect("render png");
        println!("{}", output.display());
    }
}

/// SBECAPE colorscale with mask_below(250) so low-CAPE cells render transparent
/// and the underlying land fill shows through — matches the reference images'
/// behavior where "no snowfall" or "no precipitation" cells let the basemap show.
fn cape_scale_masked() -> ColorScale {
    use rustwx_render::solar07::Solar07Preset;
    // Start from the Solar07 CAPE palette so we keep the editorial color ramp,
    // then override with mask_below.
    let base = Solar07Preset::Cape.scale();
    ColorScale::Discrete(DiscreteColorScale {
        levels: base.levels,
        colors: base.colors,
        extend: ExtendMode::Max,
        mask_below: Some(250.0),
    })
}

fn project_polygons(
    proj: &LambertConformal,
    extent: &ProjectedExtent,
    style: BasemapStyle,
) -> Vec<ProjectedPolygonFill> {
    let layers: Vec<StyledLonLatPolygonLayer> = load_styled_conus_polygons_for(style);
    let mut out = Vec::new();

    // Pad the accept window generously — polygons extend beyond the frame and
    // the scanline fill clips to image bounds anyway.
    let pad_x = 0.50 * (extent.x_max - extent.x_min);
    let pad_y = 0.50 * (extent.y_max - extent.y_min);
    let bbox = (
        extent.x_min - pad_x,
        extent.x_max + pad_x,
        extent.y_min - pad_y,
        extent.y_max + pad_y,
    );

    for layer in layers {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for polygon in layer.polygons {
            let rings: Vec<Vec<(f64, f64)>> = polygon
                .into_iter()
                .map(|ring| {
                    ring.into_iter()
                        .map(|(lon, lat)| proj.project(lat, lon))
                        .collect::<Vec<(f64, f64)>>()
                })
                .filter(|ring| ring_overlaps_bbox(ring, bbox))
                .collect();
            if rings.is_empty() {
                continue;
            }
            out.push(ProjectedPolygonFill {
                rings,
                color,
                role: layer.role,
            });
        }
    }
    out
}

fn ring_overlaps_bbox(ring: &[(f64, f64)], bbox: (f64, f64, f64, f64)) -> bool {
    let (mut rx_min, mut rx_max) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut ry_min, mut ry_max) = (f64::INFINITY, f64::NEG_INFINITY);
    for &(x, y) in ring {
        if x < rx_min {
            rx_min = x;
        }
        if x > rx_max {
            rx_max = x;
        }
        if y < ry_min {
            ry_min = y;
        }
        if y > ry_max {
            ry_max = y;
        }
    }
    !(rx_max < bbox.0 || rx_min > bbox.1 || ry_max < bbox.2 || ry_min > bbox.3)
}

fn project_lines(
    proj: &LambertConformal,
    extent: &ProjectedExtent,
    style: BasemapStyle,
) -> Vec<ProjectedLineOverlay> {
    let layers: Vec<StyledLonLatLayer> = load_styled_conus_features_for(style);
    let mut overlays = Vec::new();
    let pad_x = 0.10 * (extent.x_max - extent.x_min);
    let pad_y = 0.10 * (extent.y_max - extent.y_min);
    let x_lo = extent.x_min - pad_x;
    let x_hi = extent.x_max + pad_x;
    let y_lo = extent.y_min - pad_y;
    let y_hi = extent.y_max + pad_y;

    for layer in layers {
        let color = Color::rgba(layer.color.r, layer.color.g, layer.color.b, layer.color.a);
        for line in layer.lines {
            let mut current: Vec<(f64, f64)> = Vec::with_capacity(line.len());
            for (lon, lat) in line {
                let (x, y) = proj.project(lat, lon);
                if x < x_lo || x > x_hi || y < y_lo || y > y_hi {
                    if current.len() >= 2 {
                        overlays.push(ProjectedLineOverlay {
                            points: std::mem::take(&mut current),
                            color,
                            width: layer.width,
                            role: layer.role,
                        });
                    } else {
                        current.clear();
                    }
                    continue;
                }
                current.push((x, y));
            }
            if current.len() >= 2 {
                overlays.push(ProjectedLineOverlay {
                    points: current,
                    color,
                    width: layer.width,
                    role: layer.role,
                });
            }
        }
    }
    overlays
}

/// Newton iteration back from projected (x, y) to (lat, lon) — synthetic-only.
fn inverse_lambert(proj: &LambertConformal, x: f64, y: f64) -> (f64, f64) {
    let mut lat = REF_LAT;
    let mut lon = STAND_LON + (x / 100_000.0);
    for _ in 0..40 {
        let (px, py) = proj.project(lat, lon);
        let ex = x - px;
        let ey = y - py;
        if ex.abs() < 10.0 && ey.abs() < 10.0 {
            break;
        }
        let eps = 1e-3;
        let (px1, py1) = proj.project(lat + eps, lon);
        let (px2, py2) = proj.project(lat, lon + eps);
        let j11 = (px1 - px) / eps;
        let j12 = (px2 - px) / eps;
        let j21 = (py1 - py) / eps;
        let j22 = (py2 - py) / eps;
        let det = j11 * j22 - j12 * j21;
        if det.abs() < 1e-12 {
            break;
        }
        let dlat = (j22 * ex - j12 * ey) / det;
        let dlon = (-j21 * ex + j11 * ey) / det;
        lat += dlat.clamp(-3.0, 3.0);
        lon += dlon.clamp(-3.0, 3.0);
    }
    (lat, lon)
}

fn workspace_proof_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("proof")
}
