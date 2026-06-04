use super::*;
use crate::data::{ScalarSection, SectionMetadata, TerrainProfile};
use crate::vertical::VerticalAxis;
use crate::wind::decompose_wind_grid;

fn sample_section() -> ScalarSection {
    let axis = VerticalAxis::pressure_hpa(vec![1000.0, 900.0, 800.0, 700.0, 600.0]).unwrap();
    let mut values = Vec::new();
    for level in 0..axis.len() {
        for point in 0..6 {
            values.push(14.0 - point as f32 * 2.0 - level as f32 * 6.0);
        }
    }

    ScalarSection::new(vec![0.0, 50.0, 100.0, 150.0, 200.0, 250.0], axis, values)
        .unwrap()
        .with_metadata(
            SectionMetadata::new()
                .titled("HRRR Temperature Cross Section")
                .field("temperature", "C")
                .sourced_from("nomads")
                .valid_at("20260414 23Z F000")
                .with_attribute("start_label", "39.10N 94.58W")
                .with_attribute("end_label", "41.88N 87.63W")
                .with_attribute("route_label", "KANSAS CITY TO CHICAGO"),
        )
        .with_terrain(
            TerrainProfile::from_surface_pressure_hpa(
                vec![0.0, 50.0, 100.0, 150.0, 200.0, 250.0],
                vec![970.0, 940.0, 910.0, 905.0, 930.0, 960.0],
            )
            .unwrap(),
        )
        .unwrap()
}

fn count_exact_pixels(
    canvas: &Canvas,
    color: Color,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
) -> usize {
    let mut count = 0usize;
    for y in y_min..=y_max.min(canvas.height.saturating_sub(1)) {
        for x in x_min..=x_max.min(canvas.width.saturating_sub(1)) {
            let idx = ((y * canvas.width + x) * 4) as usize;
            if canvas.rgba[idx..idx + 4] == [color.r, color.g, color.b, 255] {
                count += 1;
            }
        }
    }
    count
}

fn count_nontransparent_pixels(
    canvas: &Canvas,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
) -> usize {
    let mut count = 0usize;
    for y in y_min..=y_max.min(canvas.height.saturating_sub(1)) {
        for x in x_min..=x_max.min(canvas.width.saturating_sub(1)) {
            let idx = ((y * canvas.width + x) * 4) as usize;
            if canvas.rgba[idx + 3] > 0 {
                count += 1;
            }
        }
    }
    count
}

fn count_exact_pixels_excluding_row(
    canvas: &Canvas,
    color: Color,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
    excluded_y: u32,
) -> usize {
    let mut count = 0usize;
    for y in y_min..=y_max.min(canvas.height.saturating_sub(1)) {
        if y == excluded_y {
            continue;
        }
        count += count_exact_pixels(canvas, color, x_min, x_max, y, y);
    }
    count
}

#[test]
fn renderer_emits_header_text_legend_and_terrain_fill() {
    let image = render_scalar_section(
        &sample_section(),
        &CrossSectionRenderRequest {
            width: 360,
            height: 220,
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(image.rgba().len(), (360 * 220 * 4) as usize);

    let header_pixels = image
        .rgba()
        .chunks_exact(4)
        .take((360 * 50) as usize)
        .filter(|px| px[0] < 80 && px[1] < 90 && px[2] < 100)
        .count();
    assert!(header_pixels > 40);

    let terrain_pixels = image
        .rgba()
        .chunks_exact(4)
        .filter(|px| px[0] >= 70 && px[1] >= 45 && px[1] <= 160 && px[2] <= 100)
        .count();
    assert!(terrain_pixels > 0);

    let legend_pixels = image
        .rgba()
        .chunks_exact(4)
        .enumerate()
        .filter(|(index, px)| {
            let x = (*index as u32) % 360;
            x >= 300 && px[0..3] != [246, 240, 231]
        })
        .count();
    assert!(legend_pixels > 0);
}

#[test]
fn renderer_draws_highlight_isotherm_overlay() {
    let image = render_scalar_section(
        &sample_section(),
        &CrossSectionRenderRequest {
            width: 360,
            height: 220,
            highlight_isotherm_c: Some(0.0),
            isotherms_c: vec![-20.0, -10.0, 0.0],
            ..Default::default()
        },
    )
    .unwrap();

    let highlight_pixels = image
        .rgba()
        .chunks_exact(4)
        .filter(|px| px[0] == 214 && px[1] == 34 && px[2] == 190)
        .count();
    assert!(highlight_pixels > 20);
}

#[test]
fn request_builders_override_ticks_and_colorbar_label() {
    let request = CrossSectionRenderRequest::default()
        .with_value_ticks(vec![-30.0, -10.0, 0.0, 10.0])
        .with_colorbar_label("Temp C")
        .with_isotherms(vec![-15.0, 0.0], Some(0.0));

    assert_eq!(request.value_ticks, vec![-30.0, -10.0, 0.0, 10.0]);
    assert_eq!(request.colorbar_label.as_deref(), Some("Temp C"));
    assert_eq!(request.isotherms_c, vec![-15.0, 0.0]);
    assert_eq!(request.highlight_isotherm_c, Some(0.0));
}

#[test]
fn wind_vector_geometry_uses_section_relative_angle() {
    let style = WindOverlayStyle::default();

    let up_right = resolve_section_wind_vector_geometry((100.0, 60.0), 12.0, 12.0, 17.0, style)
        .expect("nonzero wind should produce drawable geometry");
    assert!(up_right.end.0 > 100.0);
    assert!(up_right.end.1 < 60.0);

    let down_left = resolve_section_wind_vector_geometry((100.0, 60.0), -12.0, -12.0, 17.0, style)
        .expect("nonzero wind should produce drawable geometry");
    assert!(down_left.end.0 < 100.0);
    assert!(down_left.end.1 > 60.0);
}

#[test]
fn renderer_draws_section_relative_wind_vectors() {
    let section = sample_section();
    let wind = decompose_wind_grid(
        &[
            10.0, 10.0, 10.0, 10.0, 10.0, 10.0, //
            12.0, 12.0, 12.0, 12.0, 12.0, 12.0, //
            14.0, 14.0, 14.0, 14.0, 14.0, 14.0, //
            16.0, 16.0, 16.0, 16.0, 16.0, 16.0, //
            18.0, 18.0, 18.0, 18.0, 18.0, 18.0, //
        ],
        &[
            2.0, 2.0, 2.0, 2.0, 2.0, 2.0, //
            -2.0, -2.0, -2.0, -2.0, -2.0, -2.0, //
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0, //
            -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, //
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, //
        ],
        section.n_levels(),
        section.n_points(),
        &[90.0; 6],
    )
    .unwrap();

    let image = render_scalar_section(
        &section,
        &CrossSectionRenderRequest {
            width: 360,
            height: 220,
            wind_overlay: Some(
                WindOverlayBundle::new(
                    wind,
                    WindOverlayStyle {
                        stride_points: 2,
                        stride_levels: 1,
                        min_speed_ms: 1.0,
                        color: Color::rgb(28, 34, 43),
                        ..Default::default()
                    },
                )
                .with_label("Section Relative Wind"),
            ),
            ..Default::default()
        },
    )
    .unwrap();

    let vector_pixels = image
        .rgba()
        .chunks_exact(4)
        .filter(|px| px[0] == 28 && px[1] == 34 && px[2] == 43)
        .count();
    assert!(vector_pixels > 30);
}

#[test]
fn wind_barb_point_selection_keeps_target_columns_across_route_lengths() {
    assert_eq!(
        wind_barb_point_indices(51, 10, 6),
        vec![0, 6, 11, 17, 22, 28, 33, 39, 44, 50]
    );
    assert_eq!(wind_barb_point_indices(101, 10, 6).len(), 10);
    assert_eq!(wind_barb_point_indices(6, 10, 6), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(wind_barb_point_indices(10, 0, 4), vec![0, 4, 8]);
}

#[test]
fn wind_vector_arrowheads_flip_with_along_section_sign() {
    let mut positive_canvas = Canvas::new(80, 40, Color::TRANSPARENT, Color::TRANSPARENT);
    let mut negative_canvas = Canvas::new(80, 40, Color::TRANSPARENT, Color::TRANSPARENT);
    let plot = PlotRect {
        x: 0,
        y: 0,
        width: 80,
        height: 40,
    };
    let style = WindOverlayStyle {
        min_speed_ms: 0.0,
        max_speed_ms: 20.0,
        base_length_px: 10.0,
        max_length_px: 10.0,
        arrow_head_px: 4.0,
        line_width: 1,
        color: Color::rgb(230, 30, 30),
        ..Default::default()
    };

    draw_section_wind_vector(
        &mut positive_canvas,
        (40.0, 20.0),
        10.0,
        0.0,
        20.0,
        style,
        &plot,
    );
    draw_section_wind_vector(
        &mut negative_canvas,
        (40.0, 20.0),
        -10.0,
        0.0,
        20.0,
        style,
        &plot,
    );

    let positive_right_head =
        count_exact_pixels_excluding_row(&positive_canvas, style.color, 41, 45, 16, 24, 20);
    let positive_left_head =
        count_exact_pixels_excluding_row(&positive_canvas, style.color, 35, 39, 16, 24, 20);
    let negative_right_head =
        count_exact_pixels_excluding_row(&negative_canvas, style.color, 41, 45, 16, 24, 20);
    let negative_left_head =
        count_exact_pixels_excluding_row(&negative_canvas, style.color, 35, 39, 16, 24, 20);

    assert!(positive_right_head > 0);
    assert_eq!(positive_left_head, 0);
    assert_eq!(negative_right_head, 0);
    assert!(negative_left_head > 0);
}

#[test]
fn canvas_text_helper_renders_multiline_text_with_shadow_offset() {
    let mut canvas = Canvas::new(48, 28, Color::TRANSPARENT, Color::TRANSPARENT);
    let text_color = Color::rgb(245, 245, 245);
    let shadow_color = Color::rgb(12, 18, 24);

    canvas.draw_text(2, 2, "A\nA", text_color, 1, Some(shadow_color));

    let top_line_pixels = count_nontransparent_pixels(&canvas, 0, 47, 0, 13);
    let bottom_line_pixels = count_nontransparent_pixels(&canvas, 0, 47, 14, 27);
    let shadow_pixels = count_nontransparent_pixels(&canvas, 5, 25, 5, 25);

    assert!(top_line_pixels > 0);
    assert!(bottom_line_pixels > 0);
    assert!(shadow_pixels > 0);
}

#[test]
fn badge_rect_uses_text_width_padding_and_scale() {
    let (x, y, width, height) = badge_rect(-3, 5, "AB", 2);

    assert_eq!(x, 0);
    assert_eq!(y, 5);
    assert_eq!(width, measure_text_width("AB", 2) + 16);
    assert_eq!(height, text_line_height(2) + 4);
    assert_eq!(measure_badge_width("AB", 2), width);
}
