use crate::color::Rgba;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductVisualMode {
    FilledMeteorology,
    UpperAirAnalysis,
    OverlayAnalysis,
    SevereDiagnostic,
    PanelMember,
    ComparisonPanel,
}

impl Default for ProductVisualMode {
    fn default() -> Self {
        Self::FilledMeteorology
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineworkRole {
    Coast,
    Lake,
    International,
    State,
    County,
    Generic,
}

impl Default for LineworkRole {
    fn default() -> Self {
        Self::Generic
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolygonRole {
    Ocean,
    Land,
    Lake,
    Generic,
}

impl Default for PolygonRole {
    fn default() -> Self {
        Self::Generic
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TitleAnchor {
    Center,
    Left,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutMetrics {
    pub margin_x: u32,
    pub title_h: u32,
    pub footer_h: u32,
    pub colorbar_h: u32,
    pub colorbar_gap: u32,
    pub colorbar_margin_x: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineworkStyle {
    pub visible: bool,
    pub color: Rgba,
    pub width: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolygonStyle {
    pub visible: bool,
    pub color: Rgba,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChromeStyle {
    pub title_anchor: TitleAnchor,
    pub title_color: Rgba,
    pub subtitle_color: Rgba,
    pub frame_color: Option<Rgba>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColorbarPresentation {
    pub frame_color: Rgba,
    pub divider_color: Rgba,
    pub tick_color: Rgba,
    pub label_color: Rgba,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenderPresentation {
    pub mode: ProductVisualMode,
    pub canvas_background: Rgba,
    pub map_background: Rgba,
    pub domain_boundary: Option<LineworkStyle>,
    pub chrome: ChromeStyle,
    pub colorbar: ColorbarPresentation,
    pub layout: LayoutMetrics,
}

impl RenderPresentation {
    pub fn for_mode(mode: ProductVisualMode) -> Self {
        match mode {
            ProductVisualMode::FilledMeteorology => filled_meteorology(),
            ProductVisualMode::UpperAirAnalysis => upper_air_analysis(),
            ProductVisualMode::OverlayAnalysis => overlay_analysis(),
            ProductVisualMode::SevereDiagnostic => severe_diagnostic(),
            ProductVisualMode::PanelMember => panel_member(),
            ProductVisualMode::ComparisonPanel => comparison_panel(),
        }
    }

    pub fn polygon_style(self, role: PolygonRole, fallback: Rgba) -> PolygonStyle {
        match self.mode {
            ProductVisualMode::OverlayAnalysis => match role {
                PolygonRole::Ocean => PolygonStyle {
                    visible: true,
                    color: Rgba::new(247, 250, 253),
                },
                PolygonRole::Land => PolygonStyle {
                    visible: true,
                    color: Rgba::new(255, 255, 255),
                },
                PolygonRole::Lake => PolygonStyle {
                    visible: true,
                    color: Rgba::new(240, 247, 252),
                },
                PolygonRole::Generic => PolygonStyle {
                    visible: true,
                    color: fallback,
                },
            },
            ProductVisualMode::UpperAirAnalysis => match role {
                PolygonRole::Ocean => PolygonStyle {
                    visible: true,
                    color: Rgba::new(242, 246, 250),
                },
                PolygonRole::Land => PolygonStyle {
                    visible: true,
                    // Upper-air products can legitimately mask below-ground
                    // isobaric surfaces over higher terrain. Use a faint
                    // terrain-tinted land fill so those regions do not read as
                    // a rendering hole.
                    color: Rgba::new(232, 228, 217),
                },
                PolygonRole::Lake => PolygonStyle {
                    visible: true,
                    color: Rgba::new(232, 241, 247),
                },
                PolygonRole::Generic => PolygonStyle {
                    visible: true,
                    color: fallback,
                },
            },
            ProductVisualMode::SevereDiagnostic => match role {
                PolygonRole::Ocean => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Land => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Lake => PolygonStyle {
                    visible: true,
                    color: Rgba::new(241, 246, 250),
                },
                PolygonRole::Generic => PolygonStyle {
                    visible: true,
                    color: fallback,
                },
            },
            ProductVisualMode::PanelMember | ProductVisualMode::ComparisonPanel => match role {
                PolygonRole::Ocean => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Land => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Lake => PolygonStyle {
                    visible: true,
                    color: Rgba::new(242, 247, 250),
                },
                PolygonRole::Generic => PolygonStyle {
                    visible: true,
                    color: fallback,
                },
            },
            ProductVisualMode::FilledMeteorology => match role {
                PolygonRole::Ocean => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Land => PolygonStyle {
                    visible: false,
                    color: Rgba::TRANSPARENT,
                },
                PolygonRole::Lake => PolygonStyle {
                    visible: true,
                    color: Rgba::new(242, 247, 250),
                },
                PolygonRole::Generic => PolygonStyle {
                    visible: true,
                    color: fallback,
                },
            },
        }
    }

    pub fn linework_style(
        self,
        role: LineworkRole,
        fallback: Rgba,
        fallback_width: u32,
    ) -> LineworkStyle {
        let (color, width, visible) = match self.mode {
            ProductVisualMode::OverlayAnalysis => match role {
                LineworkRole::Coast => (Rgba::with_alpha(24, 28, 34, 210), 2, true),
                LineworkRole::Lake => (Rgba::with_alpha(40, 88, 150, 210), 2, true),
                LineworkRole::International => (Rgba::new(74, 82, 94), 1, true),
                LineworkRole::State => (Rgba::with_alpha(24, 28, 34, 210), 2, true),
                LineworkRole::County => (Rgba::with_alpha(142, 151, 162, 150), 1, true),
                LineworkRole::Generic => (fallback, fallback_width.max(1), true),
            },
            ProductVisualMode::UpperAirAnalysis => match role {
                LineworkRole::Coast => (Rgba::with_alpha(22, 26, 32, 220), 2, true),
                LineworkRole::Lake => (Rgba::with_alpha(38, 84, 146, 220), 2, true),
                LineworkRole::International => (Rgba::new(68, 76, 86), 1, true),
                LineworkRole::State => (Rgba::with_alpha(22, 26, 32, 220), 2, true),
                LineworkRole::County => (Rgba::with_alpha(150, 158, 168, 90), 1, true),
                LineworkRole::Generic => (fallback, fallback_width.max(1), true),
            },
            ProductVisualMode::SevereDiagnostic => match role {
                LineworkRole::Coast => (Rgba::with_alpha(22, 26, 31, 225), 2, true),
                LineworkRole::Lake => (Rgba::with_alpha(36, 80, 142, 225), 2, true),
                LineworkRole::International => (Rgba::new(72, 80, 88), 1, true),
                LineworkRole::State => (Rgba::with_alpha(22, 26, 31, 225), 2, true),
                LineworkRole::County => (Rgba::with_alpha(126, 134, 143, 175), 1, true),
                LineworkRole::Generic => (fallback, fallback_width.max(1), true),
            },
            ProductVisualMode::PanelMember | ProductVisualMode::ComparisonPanel => match role {
                LineworkRole::Coast => (Rgba::with_alpha(26, 30, 36, 215), 2, true),
                LineworkRole::Lake => (Rgba::with_alpha(44, 92, 154, 215), 2, true),
                LineworkRole::International => (Rgba::new(92, 100, 110), 1, true),
                LineworkRole::State => (Rgba::with_alpha(26, 30, 36, 215), 2, true),
                LineworkRole::County => (Rgba::with_alpha(150, 158, 168, 70), 1, true),
                LineworkRole::Generic => (fallback, fallback_width.max(1), true),
            },
            ProductVisualMode::FilledMeteorology => match role {
                LineworkRole::Coast => (Rgba::with_alpha(22, 26, 32, 220), 2, true),
                LineworkRole::Lake => (Rgba::with_alpha(42, 90, 152, 220), 2, true),
                LineworkRole::International => (Rgba::with_alpha(72, 80, 92, 210), 1, true),
                LineworkRole::State => (Rgba::with_alpha(22, 26, 32, 220), 2, true),
                LineworkRole::County => (Rgba::with_alpha(140, 148, 160, 70), 1, false),
                LineworkRole::Generic => (fallback, fallback_width.max(1), true),
            },
        };
        LineworkStyle {
            visible,
            color,
            width,
        }
    }

    pub fn contour_color(self, requested: Rgba) -> Rgba {
        match self.mode {
            ProductVisualMode::UpperAirAnalysis | ProductVisualMode::OverlayAnalysis => {
                Rgba::new(30, 36, 44)
            }
            ProductVisualMode::SevereDiagnostic => Rgba::new(36, 38, 40),
            ProductVisualMode::PanelMember | ProductVisualMode::ComparisonPanel => {
                if requested == Rgba::BLACK {
                    Rgba::new(42, 48, 56)
                } else {
                    requested
                }
            }
            ProductVisualMode::FilledMeteorology => requested,
        }
    }

    pub fn barb_color(self, requested: Rgba) -> Rgba {
        match self.mode {
            ProductVisualMode::UpperAirAnalysis | ProductVisualMode::OverlayAnalysis => {
                Rgba::new(28, 34, 42)
            }
            ProductVisualMode::SevereDiagnostic => Rgba::new(34, 38, 42),
            ProductVisualMode::PanelMember | ProductVisualMode::ComparisonPanel => {
                Rgba::new(44, 50, 58)
            }
            ProductVisualMode::FilledMeteorology => requested,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filled_meteorology_keeps_lake_linework_visible() {
        let style = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology)
            .linework_style(LineworkRole::Lake, Rgba::BLACK, 3);

        assert!(style.visible);
        assert_eq!(style.width, 2);
        assert_eq!(style.color, Rgba::with_alpha(42, 90, 152, 220));
    }

    #[test]
    fn filled_meteorology_uses_dark_thicker_state_lines() {
        let style = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology)
            .linework_style(LineworkRole::State, Rgba::BLACK, 1);

        assert!(style.visible);
        assert_eq!(style.width, 2);
        assert_eq!(style.color, Rgba::with_alpha(22, 26, 32, 220));
    }

    #[test]
    fn filled_meteorology_aligns_coast_and_state_lines() {
        let style = RenderPresentation::for_mode(ProductVisualMode::FilledMeteorology)
            .linework_style(LineworkRole::Coast, Rgba::BLACK, 1);

        assert!(style.visible);
        assert_eq!(style.width, 2);
        assert_eq!(style.color, Rgba::with_alpha(22, 26, 32, 220));
    }
}

fn common_chrome(title_anchor: TitleAnchor, frame_color: Option<Rgba>) -> ChromeStyle {
    ChromeStyle {
        title_anchor,
        title_color: Rgba::BLACK,
        subtitle_color: Rgba::BLACK,
        frame_color,
    }
}

fn common_colorbar() -> ColorbarPresentation {
    ColorbarPresentation {
        frame_color: Rgba::new(92, 100, 112),
        divider_color: Rgba::with_alpha(255, 255, 255, 70),
        tick_color: Rgba::new(92, 100, 112),
        label_color: Rgba::BLACK,
    }
}

fn normal_layout() -> LayoutMetrics {
    LayoutMetrics {
        margin_x: 18,
        title_h: 42,
        footer_h: 30,
        colorbar_h: 12,
        colorbar_gap: 8,
        colorbar_margin_x: 86,
    }
}

fn compact_layout() -> LayoutMetrics {
    LayoutMetrics {
        margin_x: 8,
        title_h: 34,
        footer_h: 24,
        colorbar_h: 10,
        colorbar_gap: 8,
        colorbar_margin_x: 42,
    }
}

fn filled_meteorology() -> RenderPresentation {
    RenderPresentation {
        mode: ProductVisualMode::FilledMeteorology,
        canvas_background: Rgba::new(247, 248, 250),
        map_background: Rgba::new(250, 250, 247),
        domain_boundary: None,
        chrome: common_chrome(TitleAnchor::Left, None),
        colorbar: common_colorbar(),
        layout: normal_layout(),
    }
}

fn upper_air_analysis() -> RenderPresentation {
    RenderPresentation {
        mode: ProductVisualMode::UpperAirAnalysis,
        canvas_background: Rgba::new(246, 247, 249),
        map_background: Rgba::new(238, 235, 227),
        domain_boundary: None,
        chrome: common_chrome(TitleAnchor::Left, None),
        colorbar: common_colorbar(),
        layout: normal_layout(),
    }
}

fn overlay_analysis() -> RenderPresentation {
    RenderPresentation {
        mode: ProductVisualMode::OverlayAnalysis,
        canvas_background: Rgba::WHITE,
        map_background: Rgba::WHITE,
        domain_boundary: None,
        chrome: common_chrome(TitleAnchor::Left, None),
        colorbar: common_colorbar(),
        layout: normal_layout(),
    }
}

fn severe_diagnostic() -> RenderPresentation {
    RenderPresentation {
        mode: ProductVisualMode::SevereDiagnostic,
        canvas_background: Rgba::new(247, 248, 249),
        map_background: Rgba::new(252, 253, 251),
        domain_boundary: None,
        chrome: common_chrome(TitleAnchor::Left, None),
        colorbar: common_colorbar(),
        layout: normal_layout(),
    }
}

fn panel_member() -> RenderPresentation {
    RenderPresentation {
        mode: ProductVisualMode::PanelMember,
        canvas_background: Rgba::new(246, 247, 249),
        map_background: Rgba::new(250, 250, 247),
        domain_boundary: None,
        chrome: common_chrome(TitleAnchor::Left, None),
        colorbar: common_colorbar(),
        layout: compact_layout(),
    }
}

fn comparison_panel() -> RenderPresentation {
    let mut presentation = panel_member();
    presentation.mode = ProductVisualMode::ComparisonPanel;
    presentation
}
