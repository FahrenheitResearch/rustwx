use crate::color::Rgba;

/// How to handle values outside the level range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Extend {
    Neither,
    Min,
    Max,
    Both,
}

/// A discrete colormap mapping value intervals to colours.
///
/// Given N+1 level boundaries, there are N intervals.
/// `colors` has exactly N entries (one per interval).
/// Optional `under_color` / `over_color` handle values outside the range.
#[derive(Clone, Debug)]
pub struct LeveledColormap {
    pub levels: Vec<f64>,
    pub colors: Vec<Rgba>,
    pub under_color: Option<Rgba>,
    pub over_color: Option<Rgba>,
    pub mask_below: Option<f64>,
}

impl LeveledColormap {
    /// Map a data value to a colour.
    pub fn map(&self, value: f64) -> Rgba {
        if value.is_nan() {
            return Rgba::TRANSPARENT;
        }
        if let Some(mb) = self.mask_below {
            if value < mb {
                return Rgba::TRANSPARENT;
            }
        }
        if self.levels.is_empty() || self.colors.is_empty() {
            return Rgba::TRANSPARENT;
        }
        // Below first level
        if value < self.levels[0] {
            return self.under_color.unwrap_or(Rgba::TRANSPARENT);
        }
        // Find interval via linear scan (fast for typical 20-100 levels)
        let n_intervals = self.levels.len() - 1;
        for i in 0..n_intervals {
            if value < self.levels[i + 1] {
                return self.colors[i.min(self.colors.len() - 1)];
            }
        }
        // Value == last level or above
        if value >= self.levels[n_intervals] {
            return self
                .over_color
                .unwrap_or(self.colors[self.colors.len() - 1]);
        }
        // Exact match on last boundary → last interval
        self.colors[self.colors.len() - 1]
    }

    /// Build from a palette (list of colours) and levels.
    ///
    /// Samples `palette` to produce one colour per interval, matching
    /// matplotlib's behaviour with `contourf(levels=..., cmap=cmap)`.
    pub fn from_palette(
        palette: &[Rgba],
        levels: &[f64],
        extend: Extend,
        mask_below: Option<f64>,
    ) -> Self {
        let n_intervals = if levels.len() > 1 {
            levels.len() - 1
        } else {
            0
        };
        if n_intervals == 0 || palette.is_empty() {
            return Self {
                levels: levels.to_vec(),
                colors: vec![],
                under_color: None,
                over_color: None,
                mask_below,
            };
        }

        // Sample the full palette for the actual contour intervals.
        let sampled: Vec<Rgba> = (0..n_intervals)
            .map(|i| {
                let t = if n_intervals <= 1 {
                    0.5
                } else {
                    i as f64 / (n_intervals - 1) as f64
                };
                let idx_f = t * (palette.len() - 1) as f64;
                let idx = (idx_f.round() as usize).min(palette.len() - 1);
                palette[idx]
            })
            .collect();

        let under_color = match extend {
            Extend::Min | Extend::Both => sampled.first().copied(),
            Extend::Neither | Extend::Max => None,
        };
        let over_color = match extend {
            Extend::Max | Extend::Both => sampled.last().copied(),
            Extend::Neither | Extend::Min => None,
        };

        Self {
            levels: levels.to_vec(),
            colors: sampled,
            under_color,
            over_color,
            mask_below,
        }
    }
}
