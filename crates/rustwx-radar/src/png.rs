use std::io::Cursor;

use image::{ImageBuffer, ImageFormat, Rgba};
use serde::{Deserialize, Serialize};

use crate::nexrad::derived::DerivedProducts;
use crate::nexrad::srv::SRVComputer;
use crate::nexrad::{Level2File, Level2Sweep, RadarProduct, RadarSite};
use crate::render::{ColorTable, RadarRenderer, RenderedSweep};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarFrameRender {
    pub png: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub range_km: f64,
    pub center_lat: f64,
    pub center_lon: f64,
    pub product: RadarProduct,
    pub sweep_index: usize,
    pub elevation_deg: f32,
}

#[derive(Debug, Clone, Copy)]
pub struct RadarPngOptions {
    pub size: u32,
    pub min_value: Option<f32>,
    pub draw_range_rings: bool,
    pub draw_azimuth_spokes: bool,
}

impl Default for RadarPngOptions {
    fn default() -> Self {
        Self {
            size: 1024,
            min_value: None,
            draw_range_rings: true,
            draw_azimuth_spokes: true,
        }
    }
}

pub fn render_product_png(
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    size: u32,
) -> anyhow::Result<Vec<u8>> {
    Ok(render_product_frame(
        file,
        site,
        product,
        RadarPngOptions {
            size,
            ..RadarPngOptions::default()
        },
    )?
    .png)
}

pub fn render_product_frame(
    file: &Level2File,
    site: &RadarSite,
    product: RadarProduct,
    options: RadarPngOptions,
) -> anyhow::Result<RadarFrameRender> {
    let resolved = resolve_render_sweep(file, product)?;
    let table = match options.min_value {
        Some(min_value) => ColorTable::for_product(product).with_min_value(min_value),
        None => ColorTable::for_product(product),
    };
    let rendered = RadarRenderer::render_sweep_with_table(
        resolved.sweep(),
        product,
        site,
        options.size,
        &table,
    )
    .ok_or_else(|| anyhow::anyhow!("failed to render product {}", product.short_name()))?;
    let pixels = composite_dark_frame(&rendered, options);
    let png = encode_png(&pixels, rendered.width, rendered.height)?;
    Ok(RadarFrameRender {
        png,
        width: rendered.width,
        height: rendered.height,
        range_km: rendered.range_km,
        center_lat: rendered.center_lat,
        center_lon: rendered.center_lon,
        product,
        sweep_index: resolved.sweep_index(),
        elevation_deg: resolved.sweep().elevation_angle,
    })
}

pub fn renderable_products(file: &Level2File) -> Vec<RadarProduct> {
    let mut products = file.available_products();
    if lowest_sweep_with_product(file, RadarProduct::Velocity).is_some() {
        push_unique(&mut products, RadarProduct::StormRelativeVelocity);
    }
    if lowest_sweep_with_product(file, RadarProduct::Reflectivity).is_some() {
        push_unique(&mut products, RadarProduct::VIL);
        push_unique(&mut products, RadarProduct::EchoTops);
    }
    products.sort_by_key(|product| product.short_name().to_string());
    products
}

fn push_unique(products: &mut Vec<RadarProduct>, product: RadarProduct) {
    if !products.contains(&product) {
        products.push(product);
    }
}

enum ResolvedRenderSweep<'a> {
    Borrowed {
        sweep_index: usize,
        sweep: &'a Level2Sweep,
    },
    Owned {
        sweep_index: usize,
        sweep: Level2Sweep,
    },
}

impl ResolvedRenderSweep<'_> {
    fn sweep(&self) -> &Level2Sweep {
        match self {
            Self::Borrowed { sweep, .. } => sweep,
            Self::Owned { sweep, .. } => sweep,
        }
    }

    fn sweep_index(&self) -> usize {
        match self {
            Self::Borrowed { sweep_index, .. } | Self::Owned { sweep_index, .. } => *sweep_index,
        }
    }
}

fn resolve_render_sweep(
    file: &Level2File,
    product: RadarProduct,
) -> anyhow::Result<ResolvedRenderSweep<'_>> {
    if let Some((sweep_index, sweep)) = lowest_sweep_with_product(file, product) {
        return Ok(ResolvedRenderSweep::Borrowed { sweep_index, sweep });
    }

    match product {
        RadarProduct::StormRelativeVelocity => {
            let velocity_sweeps: Vec<&Level2Sweep> = file
                .sweeps
                .iter()
                .filter(|sweep| sweep_contains_product(sweep, RadarProduct::Velocity))
                .collect();
            let (sweep_index, velocity_sweep) =
                lowest_sweep_with_product(file, RadarProduct::Velocity).ok_or_else(|| {
                    anyhow::anyhow!("cannot derive SRV because the volume has no velocity")
                })?;
            let (storm_dir_deg, storm_speed_kts) =
                SRVComputer::estimate_storm_motion(&velocity_sweeps);
            Ok(ResolvedRenderSweep::Owned {
                sweep_index,
                sweep: SRVComputer::compute(velocity_sweep, storm_dir_deg, storm_speed_kts),
            })
        }
        RadarProduct::VIL => {
            let sweep = DerivedProducts::compute_vil(file);
            ensure_nonempty_derived(product, sweep)
        }
        RadarProduct::EchoTops => {
            let sweep = DerivedProducts::compute_echo_tops(file, 18.0);
            ensure_nonempty_derived(product, sweep)
        }
        _ => Err(anyhow::anyhow!(
            "volume does not contain product {}",
            product.short_name()
        )),
    }
}

fn ensure_nonempty_derived(
    product: RadarProduct,
    sweep: Level2Sweep,
) -> anyhow::Result<ResolvedRenderSweep<'static>> {
    if sweep.radials.is_empty() {
        anyhow::bail!("cannot derive {} from this volume", product.short_name());
    }
    Ok(ResolvedRenderSweep::Owned {
        sweep_index: usize::MAX,
        sweep,
    })
}

pub fn lowest_sweep_with_product(
    file: &Level2File,
    product: RadarProduct,
) -> Option<(usize, &Level2Sweep)> {
    file.sweeps
        .iter()
        .enumerate()
        .filter(|(_, sweep)| sweep_contains_product(sweep, product))
        .min_by(|(_, a), (_, b)| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

pub fn sweep_contains_product(sweep: &Level2Sweep, product: RadarProduct) -> bool {
    sweep.radials.iter().any(|radial| {
        radial
            .moments
            .iter()
            .any(|moment| moment.product == product)
    })
}

pub fn encode_png(pixels: &[u8], width: u32, height: u32) -> anyhow::Result<Vec<u8>> {
    let image = ImageBuffer::<Rgba<u8>, _>::from_raw(width, height, pixels.to_vec())
        .ok_or_else(|| anyhow::anyhow!("invalid RGBA buffer dimensions"))?;
    let mut out = Cursor::new(Vec::new());
    image.write_to(&mut out, ImageFormat::Png)?;
    Ok(out.into_inner())
}

fn composite_dark_frame(rendered: &RenderedSweep, options: RadarPngOptions) -> Vec<u8> {
    let w = rendered.width as usize;
    let h = rendered.height as usize;
    let mut out = vec![0u8; w * h * 4];

    for y in 0..h {
        for x in 0..w {
            let idx = (y * w + x) * 4;
            out[idx] = 8;
            out[idx + 1] = 10;
            out[idx + 2] = 18;
            out[idx + 3] = 255;
        }
    }

    if options.draw_azimuth_spokes {
        for deg in (0..360).step_by(30) {
            draw_spoke(
                &mut out,
                rendered.width,
                rendered.height,
                deg as f64,
                [38, 45, 62, 150],
            );
        }
    }

    if options.draw_range_rings {
        let ring_step_km = if rendered.range_km <= 130.0 {
            25.0
        } else {
            50.0
        };
        let mut ring = ring_step_km;
        while ring < rendered.range_km {
            let radius_px = ring / rendered.range_km * rendered.width as f64 / 2.0;
            draw_ring(
                &mut out,
                rendered.width,
                rendered.height,
                radius_px,
                [44, 52, 72, 170],
            );
            ring += ring_step_km;
        }
    }

    for i in 0..(w * h) {
        let src_idx = i * 4;
        let a = rendered.pixels[src_idx + 3];
        if a == 0 {
            continue;
        }
        blend_pixel_raw(
            &mut out,
            src_idx,
            [
                rendered.pixels[src_idx],
                rendered.pixels[src_idx + 1],
                rendered.pixels[src_idx + 2],
                a,
            ],
        );
    }

    draw_ring(
        &mut out,
        rendered.width,
        rendered.height,
        rendered.width as f64 / 2.0 - 1.0,
        [72, 82, 110, 220],
    );

    out
}

fn draw_ring(pixels: &mut [u8], width: u32, height: u32, radius_px: f64, color: [u8; 4]) {
    let cx = width as f64 / 2.0;
    let cy = height as f64 / 2.0;
    let steps = (radius_px * 2.0 * std::f64::consts::PI).round().max(120.0) as usize;
    for i in 0..steps {
        let theta = i as f64 / steps as f64 * std::f64::consts::TAU;
        let x = (cx + radius_px * theta.cos()).round() as i32;
        let y = (cy + radius_px * theta.sin()).round() as i32;
        blend_pixel(pixels, width, height, x, y, color);
    }
}

fn draw_spoke(pixels: &mut [u8], width: u32, height: u32, azimuth_deg: f64, color: [u8; 4]) {
    let cx = width as f64 / 2.0;
    let cy = height as f64 / 2.0;
    let max_r = width.min(height) as f64 / 2.0;
    let az = azimuth_deg.to_radians();
    let dx = az.sin();
    let dy = -az.cos();
    for r in (0..max_r as i32).step_by(2) {
        let x = (cx + dx * r as f64).round() as i32;
        let y = (cy + dy * r as f64).round() as i32;
        blend_pixel(pixels, width, height, x, y, color);
    }
}

fn blend_pixel(pixels: &mut [u8], width: u32, height: u32, x: i32, y: i32, color: [u8; 4]) {
    if x < 0 || y < 0 || x >= width as i32 || y >= height as i32 {
        return;
    }
    let idx = ((y as u32 * width + x as u32) * 4) as usize;
    blend_pixel_raw(pixels, idx, color);
}

fn blend_pixel_raw(pixels: &mut [u8], idx: usize, color: [u8; 4]) {
    let a = color[3] as f32 / 255.0;
    let inv = 1.0 - a;
    pixels[idx] = (pixels[idx] as f32 * inv + color[0] as f32 * a).round() as u8;
    pixels[idx + 1] = (pixels[idx + 1] as f32 * inv + color[1] as f32 * a).round() as u8;
    pixels[idx + 2] = (pixels[idx + 2] as f32 * inv + color[2] as f32 * a).round() as u8;
    pixels[idx + 3] = 255;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nexrad::level2::{MomentData, RadialData};

    #[test]
    fn renders_synthetic_reflectivity_png() {
        let sweep = synthetic_reflectivity_sweep();
        let file = Level2File {
            station_id: "KTLX".to_string(),
            volume_date: 20_000,
            volume_time: 0,
            vcp: None,
            sweeps: vec![sweep],
            partial: false,
        };
        let site = RadarSite {
            id: "KTLX",
            name: "Oklahoma City",
            lat: 35.333,
            lon: -97.277,
            state: "OK",
        };
        let png = render_product_png(&file, &site, RadarProduct::Reflectivity, 256).unwrap();
        assert!(png.starts_with(b"\x89PNG"));
        assert!(png.len() > 2_000);
    }

    fn synthetic_reflectivity_sweep() -> Level2Sweep {
        let mut radials = Vec::new();
        for az in 0..360 {
            let mut data = vec![f32::NAN; 240];
            for gate in 30..110 {
                let az_dist = ((az as f32 - 225.0 + 540.0) % 360.0 - 180.0).abs();
                if az_dist < 18.0 {
                    data[gate] = 55.0 - az_dist * 0.6 - (gate as f32 - 70.0).abs() * 0.15;
                }
            }
            radials.push(RadialData {
                azimuth: az as f32,
                elevation: 0.5,
                azimuth_spacing: 1.0,
                nyquist_velocity: None,
                radial_status: 1,
                moments: vec![MomentData {
                    product: RadarProduct::Reflectivity,
                    gate_count: data.len() as u16,
                    first_gate_range: 0,
                    gate_size: 1_000,
                    data,
                }],
            });
        }
        Level2Sweep {
            elevation_number: 1,
            elevation_angle: 0.5,
            nyquist_velocity: None,
            radials,
        }
    }
}
