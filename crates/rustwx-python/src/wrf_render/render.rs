use image::ExtendedColorType;
use image::ImageEncoder;
use image::RgbaImage;
use image::codecs::png::{CompressionType, FilterType as PngFilterType, PngEncoder};
use image::imageops::crop_imm;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use rustwx_render::{
    Color, DomainFrame, Field2D, GridShape, LatLonGrid, LevelDensity, MapRenderRequest, ProductKey,
    ProductVisualMode, ProjectedDomain, RenderDensity, RenderPresentation, render_image,
};
use serde::Serialize;
use std::fs;
use std::path::Path;

use super::projection::{
    Array2Data, ProjectedRenderArrays, geometry_metadata, prepare_projected_surface, project_lines,
    project_polygons, projector_from_spec, validate_projected_render_arrays,
};
use super::spec::{
    OverlaySpec, ProjectedMapRenderMetadata, ProjectedSurfaceSpec, RenderLayersMetadata, RenderSpec,
};

fn default_projected_render_density() -> RenderDensity {
    RenderDensity {
        fill: LevelDensity::default(),
        palette_multiplier: 1,
    }
}

fn build_request(
    spec: &RenderSpec,
    grid: &LatLonGrid,
    field_values: Vec<f32>,
    geometry: &super::projection::Geometry,
    visual_mode: ProductVisualMode,
) -> PyResult<MapRenderRequest> {
    let field = Field2D::new(
        ProductKey::named(spec.product_key.clone()),
        spec.field_units.clone(),
        grid.clone(),
        field_values,
    )
    .map_err(to_runtime_error)?;
    let mut request = MapRenderRequest::new(field, spec.scale.clone().into_color_scale());
    request.width = spec.width.unwrap_or(1100);
    request.height = spec.height.unwrap_or(850);
    request.render_density = default_projected_render_density();
    request.colorbar = spec.colorbar.unwrap_or(true);
    request.title = spec.title.clone();
    request.subtitle_left = spec.subtitle_left.clone();
    request.subtitle_center = spec.subtitle_center.clone();
    request.subtitle_right = spec.subtitle_right.clone();
    request.cbar_tick_step = spec.tick_step;
    request.visual_mode = visual_mode;
    request.domain_frame = if spec.domain_frame.unwrap_or(true) {
        Some(DomainFrame::model_data_default())
    } else {
        None
    };
    request.projected_domain = Some(ProjectedDomain {
        x: geometry.x.clone(),
        y: geometry.y.clone(),
        extent: geometry.padded_extent.clone(),
    });
    if let Some(style) = spec.basemap_style.unwrap_or_default().to_option() {
        request.projected_polygons = project_polygons(
            projector_from_spec(&spec.projection)?,
            &geometry.padded_extent,
            style,
        );
        request.projected_lines = project_lines(
            projector_from_spec(&spec.projection)?,
            &geometry.padded_extent,
            style,
        );
    }
    Ok(request)
}

fn build_overlay_request(
    spec: &RenderSpec,
    grid: &LatLonGrid,
    field_values: Vec<f32>,
    geometry: &super::projection::Geometry,
    overlay: &OverlaySpec,
) -> PyResult<MapRenderRequest> {
    let field = Field2D::new(
        ProductKey::named(format!("{} Overlay", spec.product_key)),
        spec.field_units.clone(),
        grid.clone(),
        field_values,
    )
    .map_err(to_runtime_error)?;
    let visual_mode = overlay
        .visual_mode
        .unwrap_or(ProductVisualMode::OverlayAnalysis);
    let mut request = MapRenderRequest::new(field, overlay.scale.clone().into_color_scale());
    request.width = spec.width.unwrap_or(1100);
    request.height = spec.height.unwrap_or(850);
    request.render_density = default_projected_render_density();
    request.background = Color::TRANSPARENT;
    request.colorbar = false;
    request.title = Some(String::new());
    request.subtitle_left = Some(String::new());
    request.subtitle_center = Some(String::new());
    request.subtitle_right = Some(String::new());
    request.visual_mode = visual_mode;
    request.domain_frame = if spec.domain_frame.unwrap_or(true) {
        Some(DomainFrame::model_data_default())
    } else {
        None
    };
    request.projected_domain = Some(ProjectedDomain {
        x: geometry.x.clone(),
        y: geometry.y.clone(),
        extent: geometry.padded_extent.clone(),
    });
    Ok(request)
}

fn add_contour_layer(
    request: &mut MapRenderRequest,
    grid: &LatLonGrid,
    spec: &RenderSpec,
    contour_field: &Array2Data,
) -> PyResult<()> {
    let contour_spec = spec
        .contour
        .as_ref()
        .ok_or_else(|| PyValueError::new_err("Contour field provided without contour spec"))?;
    let field = Field2D::new(
        ProductKey::named(format!("{} Contours", spec.product_key)),
        spec.field_units.clone(),
        grid.clone(),
        contour_field.to_f32(),
    )
    .map_err(to_runtime_error)?;
    request
        .add_contour_field(
            &field,
            contour_spec.levels.clone(),
            contour_spec.style.unwrap_or_default(),
        )
        .map_err(to_runtime_error)?;
    Ok(())
}

fn add_wind_barbs(
    request: &mut MapRenderRequest,
    grid: &LatLonGrid,
    spec: &RenderSpec,
    wind_u: &Array2Data,
    wind_v: &Array2Data,
) -> PyResult<()> {
    let style = spec.wind_barbs.unwrap_or_default();
    let u_field = Field2D::new(
        ProductKey::named(format!("{} Wind U", spec.product_key)),
        "kt",
        grid.clone(),
        wind_u.to_f32(),
    )
    .map_err(to_runtime_error)?;
    let v_field = Field2D::new(
        ProductKey::named(format!("{} Wind V", spec.product_key)),
        "kt",
        grid.clone(),
        wind_v.to_f32(),
    )
    .map_err(to_runtime_error)?;
    request
        .add_wind_barbs(&u_field, &v_field, style)
        .map_err(to_runtime_error)?;
    Ok(())
}

fn alpha_composite(base: &mut RgbaImage, overlay: &RgbaImage) {
    let width = base.width().min(overlay.width());
    let height = base.height().min(overlay.height());
    for y in 0..height {
        for x in 0..width {
            let dst = *base.get_pixel(x, y);
            let src = *overlay.get_pixel(x, y);
            let src_a = src.0[3] as f32 / 255.0;
            if src_a <= 0.0 {
                continue;
            }
            let dst_a = dst.0[3] as f32 / 255.0;
            let out_a = src_a + dst_a * (1.0 - src_a);
            let mut out = [0_u8; 4];
            for channel in 0..3 {
                let src_c = src.0[channel] as f32 / 255.0;
                let dst_c = dst.0[channel] as f32 / 255.0;
                let value = if out_a <= 0.0 {
                    0.0
                } else {
                    (src_c * src_a + dst_c * dst_a * (1.0 - src_a)) / out_a
                };
                out[channel] = (value * 255.0).round().clamp(0.0, 255.0) as u8;
            }
            out[3] = (out_a * 255.0).round().clamp(0.0, 255.0) as u8;
            base.put_pixel(x, y, image::Rgba(out));
        }
    }
}

fn strip_overlay_background(image: &mut RgbaImage) {
    for pixel in image.pixels_mut() {
        let [r, g, b, _] = pixel.0;
        if u16::from(r.abs_diff(255)) + u16::from(g.abs_diff(255)) + u16::from(b.abs_diff(255)) <= 6
        {
            *pixel = image::Rgba([0, 0, 0, 0]);
        }
    }
}

fn row_is_background(image: &RgbaImage, y: u32, background: [u8; 4]) -> bool {
    (0..image.width()).all(|x| {
        let px = image.get_pixel(x, y).0;
        let diff = u16::from(px[0].abs_diff(background[0]))
            + u16::from(px[1].abs_diff(background[1]))
            + u16::from(px[2].abs_diff(background[2]))
            + u16::from(px[3].abs_diff(background[3]));
        diff <= 6
    })
}

fn trim_vertical_canvas_whitespace(image: &RgbaImage, background: [u8; 4]) -> (RgbaImage, u32) {
    if image.height() <= 2 {
        return (image.clone(), 0);
    }
    let first_non_bg = (0..image.height()).find(|&y| !row_is_background(image, y, background));
    let last_non_bg = (0..image.height()).rfind(|&y| !row_is_background(image, y, background));
    let (Some(first), Some(last)) = (first_non_bg, last_non_bg) else {
        return (image.clone(), 0);
    };
    let crop_top = first.saturating_sub(2);
    let crop_bottom = last.saturating_add(2).min(image.height().saturating_sub(1));
    let crop_h = crop_bottom.saturating_sub(crop_top).saturating_add(1);
    if crop_top == 0 && crop_h == image.height() {
        return (image.clone(), 0);
    }
    (
        crop_imm(image, 0, crop_top, image.width(), crop_h).to_image(),
        crop_top,
    )
}

fn write_png(path: &Path, image: &RgbaImage) -> PyResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    let mut bytes = Vec::new();
    let encoder = PngEncoder::new_with_quality(
        &mut bytes,
        CompressionType::Default,
        PngFilterType::Adaptive,
    );
    encoder
        .write_image(
            image.as_raw(),
            image.width(),
            image.height(),
            ExtendedColorType::Rgba8,
        )
        .map_err(io_error)?;
    fs::write(path, bytes).map_err(io_error)
}

fn io_error<E: std::fmt::Display>(error: E) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

pub(crate) fn to_runtime_error<E: std::fmt::Display>(error: E) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

pub(crate) fn serialize_pretty<T: Serialize>(value: &T) -> PyResult<String> {
    serde_json::to_string_pretty(value).map_err(to_runtime_error)
}

pub(crate) fn render_projected_map_impl(
    spec: &RenderSpec,
    arrays: ProjectedRenderArrays,
) -> PyResult<ProjectedMapRenderMetadata> {
    validate_projected_render_arrays(spec, &arrays)?;
    let surface_spec = ProjectedSurfaceSpec::from(spec);
    let (projector, geometry, layout, context) =
        prepare_projected_surface(&surface_spec, &arrays.lat, &arrays.lon)?;

    let shape = GridShape::new(arrays.field.nx, arrays.field.ny).map_err(to_runtime_error)?;
    let grid = LatLonGrid::new(shape, arrays.lat.to_f32(), arrays.lon.to_f32())
        .map_err(to_runtime_error)?;

    let mut request = build_request(
        spec,
        &grid,
        arrays.field.to_f32(),
        &geometry,
        context.visual_mode,
    )?;
    if spec.overlay.is_none() {
        if let Some(ref contour_field) = arrays.contour_field {
            add_contour_layer(&mut request, &grid, spec, contour_field)?;
        }
    }
    if let (Some(wind_u), Some(wind_v)) = (&arrays.wind_u, &arrays.wind_v) {
        add_wind_barbs(&mut request, &grid, spec, wind_u, wind_v)?;
    }

    let mut image = render_image(&request).map_err(to_runtime_error)?;
    if let Some(ref overlay_spec) = spec.overlay {
        let overlay_field = arrays.overlay_field.as_ref().ok_or_else(|| {
            PyValueError::new_err("Overlay spec provided without overlay field array")
        })?;
        let mut overlay_request =
            build_overlay_request(spec, &grid, overlay_field.to_f32(), &geometry, overlay_spec)?;
        if let Some(ref contour_field) = arrays.contour_field {
            add_contour_layer(&mut overlay_request, &grid, spec, contour_field)?;
        }
        let mut overlay_image = render_image(&overlay_request).map_err(to_runtime_error)?;
        strip_overlay_background(&mut overlay_image);
        alpha_composite(&mut image, &overlay_image);
    }

    let background = RenderPresentation::for_mode(context.visual_mode)
        .canvas_background
        .to_image_rgba()
        .0;
    let (trimmed, crop_top) = trim_vertical_canvas_whitespace(&image, background);
    write_png(Path::new(&spec.output_path), &trimmed)?;

    let base = geometry_metadata(
        "projected_map_render",
        &surface_spec,
        projector,
        &geometry,
        layout,
        crop_top,
        &arrays.lat,
        false,
    );
    Ok(ProjectedMapRenderMetadata {
        kind: "projected_map_render",
        schema_version: 1,
        output_path: spec.output_path.clone(),
        grid_shape: base.grid_shape,
        pixel_bounds: base.pixel_bounds,
        data_extent: base.data_extent,
        valid_data_extent: base.valid_data_extent,
        projection_info: base.projection_info,
        projection: base.projection,
        extents: base.extents,
        layout: base.layout,
        projected_corners: base.projected_corners,
        layers: RenderLayersMetadata {
            basemap_style: surface_spec.basemap_style(),
            contours: arrays.contour_field.is_some(),
            overlay_fill: spec.overlay.is_some(),
            wind_barbs: arrays.wind_u.is_some(),
        },
    })
}
