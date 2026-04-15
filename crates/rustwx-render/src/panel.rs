use image::{GenericImage, Rgba, RgbaImage};

use crate::{Color, MapRenderRequest, RustwxRenderError, render_image};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PanelPadding {
    pub top: u32,
    pub right: u32,
    pub bottom: u32,
    pub left: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PanelGridLayout {
    pub rows: u32,
    pub columns: u32,
    pub panel_width: u32,
    pub panel_height: u32,
    pub gap_x: u32,
    pub gap_y: u32,
    pub padding: PanelPadding,
    pub background: Color,
}

impl PanelGridLayout {
    pub fn new(
        rows: u32,
        columns: u32,
        panel_width: u32,
        panel_height: u32,
    ) -> Result<Self, RustwxRenderError> {
        if rows == 0 || columns == 0 || panel_width == 0 || panel_height == 0 {
            return Err(RustwxRenderError::InvalidPanelLayout {
                rows,
                columns,
                panel_width,
                panel_height,
            });
        }

        Ok(Self {
            rows,
            columns,
            panel_width,
            panel_height,
            gap_x: 0,
            gap_y: 0,
            padding: PanelPadding::default(),
            background: Color::WHITE,
        })
    }

    pub fn two_by_two(panel_width: u32, panel_height: u32) -> Result<Self, RustwxRenderError> {
        Self::new(2, 2, panel_width, panel_height)
    }

    pub fn two_by_four(panel_width: u32, panel_height: u32) -> Result<Self, RustwxRenderError> {
        Self::new(2, 4, panel_width, panel_height)
    }

    pub fn with_gaps(mut self, gap_x: u32, gap_y: u32) -> Self {
        self.gap_x = gap_x;
        self.gap_y = gap_y;
        self
    }

    pub fn with_padding(mut self, padding: PanelPadding) -> Self {
        self.padding = padding;
        self
    }

    pub fn with_background(mut self, background: Color) -> Self {
        self.background = background;
        self
    }

    pub fn capacity(self) -> usize {
        (self.rows as usize) * (self.columns as usize)
    }

    pub fn canvas_size(self) -> Result<(u32, u32), RustwxRenderError> {
        let width = axis_span(
            self.padding.left,
            self.columns,
            self.panel_width,
            self.gap_x,
            self.padding.right,
        )?;
        let height = axis_span(
            self.padding.top,
            self.rows,
            self.panel_height,
            self.gap_y,
            self.padding.bottom,
        )?;

        Ok((width, height))
    }

    pub fn panel_origin(self, index: usize) -> Result<(u32, u32), RustwxRenderError> {
        let capacity = self.capacity();
        if index >= capacity {
            return Err(RustwxRenderError::TooManyPanels {
                actual: index + 1,
                capacity,
            });
        }

        let row = index as u32 / self.columns;
        let column = index as u32 % self.columns;
        let x_stride = self
            .panel_width
            .checked_add(self.gap_x)
            .ok_or(RustwxRenderError::PanelLayoutOverflow)?;
        let y_stride = self
            .panel_height
            .checked_add(self.gap_y)
            .ok_or(RustwxRenderError::PanelLayoutOverflow)?;
        let x = self
            .padding
            .left
            .checked_add(
                column
                    .checked_mul(x_stride)
                    .ok_or(RustwxRenderError::PanelLayoutOverflow)?,
            )
            .ok_or(RustwxRenderError::PanelLayoutOverflow)?;
        let y = self
            .padding
            .top
            .checked_add(
                row.checked_mul(y_stride)
                    .ok_or(RustwxRenderError::PanelLayoutOverflow)?,
            )
            .ok_or(RustwxRenderError::PanelLayoutOverflow)?;
        Ok((x, y))
    }
}

pub fn compose_panel_images(
    layout: &PanelGridLayout,
    panels: &[RgbaImage],
) -> Result<RgbaImage, RustwxRenderError> {
    if panels.len() > layout.capacity() {
        return Err(RustwxRenderError::TooManyPanels {
            actual: panels.len(),
            capacity: layout.capacity(),
        });
    }

    let (canvas_width, canvas_height) = layout.canvas_size()?;
    let mut canvas = RgbaImage::from_pixel(
        canvas_width,
        canvas_height,
        Rgba([
            layout.background.r,
            layout.background.g,
            layout.background.b,
            layout.background.a,
        ]),
    );

    for (index, panel) in panels.iter().enumerate() {
        validate_panel_size(layout, index, panel.width(), panel.height())?;
        let (x, y) = layout.panel_origin(index)?;
        canvas
            .copy_from(panel, x, y)
            .map_err(|source| RustwxRenderError::ComposePanel { index, source })?;
    }

    Ok(canvas)
}

pub fn render_panel_grid(
    layout: &PanelGridLayout,
    requests: &[MapRenderRequest],
) -> Result<RgbaImage, RustwxRenderError> {
    if requests.len() > layout.capacity() {
        return Err(RustwxRenderError::TooManyPanels {
            actual: requests.len(),
            capacity: layout.capacity(),
        });
    }

    let mut panels = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        validate_panel_size(layout, index, request.width, request.height)?;
        panels.push(render_image(request)?);
    }

    compose_panel_images(layout, &panels)
}

fn validate_panel_size(
    layout: &PanelGridLayout,
    index: usize,
    actual_width: u32,
    actual_height: u32,
) -> Result<(), RustwxRenderError> {
    if actual_width != layout.panel_width || actual_height != layout.panel_height {
        return Err(RustwxRenderError::PanelSizeMismatch {
            index,
            expected_width: layout.panel_width,
            expected_height: layout.panel_height,
            actual_width,
            actual_height,
        });
    }
    Ok(())
}

fn axis_span(
    start_padding: u32,
    count: u32,
    item_size: u32,
    gap: u32,
    end_padding: u32,
) -> Result<u32, RustwxRenderError> {
    let item_total = count
        .checked_mul(item_size)
        .ok_or(RustwxRenderError::PanelLayoutOverflow)?;
    let gap_total = count
        .checked_sub(1)
        .ok_or(RustwxRenderError::PanelLayoutOverflow)?
        .checked_mul(gap)
        .ok_or(RustwxRenderError::PanelLayoutOverflow)?;

    start_padding
        .checked_add(item_total)
        .and_then(|value| value.checked_add(gap_total))
        .and_then(|value| value.checked_add(end_padding))
        .ok_or(RustwxRenderError::PanelLayoutOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ColorScale, ContourStyle, Field2D, GridShape, LatLonGrid, ProductKey, ProjectedDomain,
        ProjectedExtent, ProjectedLineOverlay, WindBarbStyle,
    };

    fn solid_panel(width: u32, height: u32, rgba: [u8; 4]) -> RgbaImage {
        RgbaImage::from_pixel(width, height, Rgba(rgba))
    }

    fn sample_request(product: &str, width: u32, height: u32) -> MapRenderRequest {
        let shape = GridShape::new(3, 2).unwrap();
        let grid = LatLonGrid::new(
            shape,
            vec![35.0, 35.0, 35.0, 36.0, 36.0, 36.0],
            vec![-99.0, -98.0, -97.0, -99.0, -98.0, -97.0],
        )
        .unwrap();
        let field = Field2D::new(
            ProductKey::named(product),
            "J/kg",
            grid,
            vec![0.0, 250.0, 500.0, 750.0, 1000.0, 1250.0],
        )
        .unwrap();
        let mut request = MapRenderRequest::new(
            field,
            ColorScale::Solar07(crate::solar07::Solar07Preset::Cape),
        );
        request.width = width;
        request.height = height;
        request.colorbar = false;
        request
    }

    fn sample_projected_request(product: &str, width: u32, height: u32) -> MapRenderRequest {
        let mut request = sample_request(product, width, height);
        request.projected_domain = Some(ProjectedDomain {
            x: vec![0.0, 1.0, 2.0, 0.0, 1.0, 2.0],
            y: vec![0.0, 0.0, 0.0, 1.0, 1.0, 1.0],
            extent: ProjectedExtent {
                x_min: 0.0,
                x_max: 2.0,
                y_min: 0.0,
                y_max: 1.0,
            },
        });
        request.projected_lines = vec![ProjectedLineOverlay {
            points: vec![(0.0, 0.0), (2.0, 1.0)],
            color: Color::BLACK,
            width: 2,
        }];
        request
    }

    #[test]
    fn compose_panel_images_places_row_major_panels_with_padding_and_gaps() {
        let layout = PanelGridLayout::two_by_two(4, 3)
            .unwrap()
            .with_gaps(1, 2)
            .with_padding(PanelPadding {
                top: 5,
                right: 4,
                bottom: 3,
                left: 2,
            })
            .with_background(Color::BLACK);

        let red = solid_panel(4, 3, [255, 0, 0, 255]);
        let green = solid_panel(4, 3, [0, 255, 0, 255]);
        let blue = solid_panel(4, 3, [0, 0, 255, 255]);
        let yellow = solid_panel(4, 3, [255, 255, 0, 255]);
        let canvas = compose_panel_images(&layout, &[red, green, blue, yellow]).unwrap();

        assert_eq!(canvas.width(), 15);
        assert_eq!(canvas.height(), 16);
        assert_eq!(canvas.get_pixel(0, 0).0, [0, 0, 0, 255]);
        assert_eq!(canvas.get_pixel(2, 5).0, [255, 0, 0, 255]);
        assert_eq!(canvas.get_pixel(7, 5).0, [0, 255, 0, 255]);
        assert_eq!(canvas.get_pixel(2, 10).0, [0, 0, 255, 255]);
        assert_eq!(canvas.get_pixel(7, 10).0, [255, 255, 0, 255]);
    }

    #[test]
    fn compose_panel_images_rejects_size_mismatch() {
        let layout = PanelGridLayout::two_by_two(4, 3).unwrap();
        let panels = [
            solid_panel(4, 3, [255, 0, 0, 255]),
            solid_panel(5, 3, [0, 255, 0, 255]),
        ];
        let error = compose_panel_images(&layout, &panels).unwrap_err();

        assert!(matches!(
            error,
            RustwxRenderError::PanelSizeMismatch {
                index: 1,
                expected_width: 4,
                expected_height: 3,
                actual_width: 5,
                actual_height: 3,
            }
        ));
    }

    #[test]
    fn render_panel_grid_leaves_unused_slots_as_background() {
        let layout = PanelGridLayout::two_by_two(140, 100)
            .unwrap()
            .with_background(Color::rgba(240, 240, 240, 255));
        let requests = [
            sample_request("sbecape", 140, 100),
            sample_request("mlecape", 140, 100),
        ];

        let canvas = render_panel_grid(&layout, &requests).unwrap();

        assert_eq!(canvas.width(), 280);
        assert_eq!(canvas.height(), 200);
        assert_eq!(canvas.get_pixel(210, 150).0, [240, 240, 240, 255]);
        let non_background = canvas
            .pixels()
            .filter(|px| px.0 != [240, 240, 240, 255])
            .count();
        assert!(
            non_background > 5000,
            "rendered panel grid should contain plot content"
        );
    }

    #[test]
    fn render_panel_grid_renders_repeated_projected_domains() {
        let layout = PanelGridLayout::new(1, 2, 140, 100)
            .unwrap()
            .with_background(Color::rgba(245, 245, 245, 255));
        let requests = [
            sample_projected_request("sbecape", 140, 100),
            sample_projected_request("mlecape", 140, 100),
        ];

        let canvas = render_panel_grid(&layout, &requests).unwrap();

        assert_eq!(canvas.width(), 280);
        assert_eq!(canvas.height(), 100);
        let non_background = canvas
            .pixels()
            .filter(|px| px.0 != [245, 245, 245, 255])
            .count();
        assert!(
            non_background > 7000,
            "projected multi-panel render should contain plot and overlay content"
        );
    }

    #[test]
    fn render_panel_grid_supports_mixed_filled_and_overlay_only_requests() {
        let layout = PanelGridLayout::new(1, 2, 160, 120).unwrap();
        let filled = sample_request("temperature", 160, 120);
        let height_field = sample_request("height", 160, 120).field;
        let contour_field = sample_request("height_contours", 160, 120).field;
        let u_field = sample_request("u10", 160, 120).field;
        let v_field = sample_request("v10", 160, 120).field;
        let mut overlay_only = crate::MapRenderRequest::contour_only(height_field)
            .with_contour_field(
                &contour_field,
                vec![250.0, 500.0, 750.0, 1000.0],
                ContourStyle {
                    labels: true,
                    ..Default::default()
                },
            )
            .unwrap()
            .with_wind_barbs(
                &u_field,
                &v_field,
                WindBarbStyle {
                    stride_x: 1,
                    stride_y: 1,
                    length_px: 14.0,
                    ..Default::default()
                },
            )
            .unwrap();
        overlay_only.width = 160;
        overlay_only.height = 120;

        let canvas = render_panel_grid(&layout, &[filled, overlay_only]).unwrap();
        assert_eq!(canvas.width(), 320);
        assert_eq!(canvas.height(), 120);
        let non_white = canvas
            .pixels()
            .filter(|px| px.0 != [255, 255, 255, 255])
            .count();
        assert!(non_white > 10000);
    }
}
