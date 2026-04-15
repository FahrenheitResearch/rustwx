use rustwx_core::GridShape;
use std::collections::VecDeque;
use thiserror::Error;

pub const WRF_WATER_LU_CATEGORIES: [i32; 3] = [16, 17, 21];

#[derive(Debug, Error)]
pub enum PrepError {
    #[error("invalid lake interpolation area threshold {0}; expected a finite positive km^2 value")]
    InvalidAreaThresholdKm2(f64),
    #[error("invalid grid spacing dx={dx_m} dy={dy_m}; expected finite positive meters")]
    InvalidGridSpacingMeters { dx_m: f64, dy_m: f64 },
    #[error("grid/data length mismatch: expected {expected}, got {actual}")]
    InvalidGridLength { expected: usize, actual: usize },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WrfLakeMaskSpec {
    pub shape: GridShape,
    pub dx_m: f64,
    pub dy_m: f64,
    pub area_threshold_km2: f64,
}

impl WrfLakeMaskSpec {
    pub fn new(
        shape: GridShape,
        dx_m: f64,
        dy_m: f64,
        area_threshold_km2: f64,
    ) -> Result<Self, PrepError> {
        if !dx_m.is_finite() || !dy_m.is_finite() || dx_m <= 0.0 || dy_m <= 0.0 {
            return Err(PrepError::InvalidGridSpacingMeters { dx_m, dy_m });
        }
        if !area_threshold_km2.is_finite() || area_threshold_km2 <= 0.0 {
            return Err(PrepError::InvalidAreaThresholdKm2(area_threshold_km2));
        }
        Ok(Self {
            shape,
            dx_m,
            dy_m,
            area_threshold_km2,
        })
    }

    pub fn grid_area_km2(self) -> f64 {
        (self.dx_m * self.dy_m) / 1e6
    }

    pub fn cell_count_threshold(self) -> usize {
        (self.area_threshold_km2 / self.grid_area_km2()) as usize
    }
}

pub fn wrf_small_water_mask(
    lu_index: &[f32],
    spec: WrfLakeMaskSpec,
) -> Result<Vec<bool>, PrepError> {
    validate_len(lu_index.len(), spec.shape.len())?;

    let nx = spec.shape.nx;
    let ny = spec.shape.ny;
    let is_water: Vec<bool> = lu_index
        .iter()
        .map(|value| WRF_WATER_LU_CATEGORIES.contains(&(*value as i32)))
        .collect();

    let mut labels = vec![0u32; spec.shape.len()];
    let mut current_label = 0u32;
    let mut label_sizes = vec![0usize];

    for start in 0..spec.shape.len() {
        if !is_water[start] || labels[start] != 0 {
            continue;
        }
        current_label += 1;
        let mut size = 0usize;
        let mut stack = vec![start];

        while let Some(idx) = stack.pop() {
            if labels[idx] != 0 {
                continue;
            }
            labels[idx] = current_label;
            size += 1;

            let j = idx / nx;
            let i = idx % nx;
            for dj in [-1i32, 0, 1] {
                for di in [-1i32, 0, 1] {
                    if dj == 0 && di == 0 {
                        continue;
                    }
                    let nj = j as i32 + dj;
                    let ni = i as i32 + di;
                    if nj < 0 || nj >= ny as i32 || ni < 0 || ni >= nx as i32 {
                        continue;
                    }
                    let nidx = nj as usize * nx + ni as usize;
                    if is_water[nidx] && labels[nidx] == 0 {
                        stack.push(nidx);
                    }
                }
            }
        }

        label_sizes.push(size);
    }

    let count_threshold = spec.cell_count_threshold();
    Ok(labels
        .iter()
        .map(|&label| {
            if label == 0 {
                return false;
            }
            label_sizes[label as usize] < count_threshold
        })
        .collect())
}

pub fn interpolate_masked_2d_f32(
    data: &[f32],
    mask: &[bool],
    shape: GridShape,
) -> Result<Vec<f32>, PrepError> {
    validate_len(data.len(), shape.len())?;
    validate_len(mask.len(), shape.len())?;
    Ok(interpolate_masked_2d_impl_f32(
        data, mask, shape.ny, shape.nx,
    ))
}

pub fn interpolate_masked_2d_f64(
    data: &[f64],
    mask: &[bool],
    shape: GridShape,
) -> Result<Vec<f64>, PrepError> {
    validate_len(data.len(), shape.len())?;
    validate_len(mask.len(), shape.len())?;
    Ok(interpolate_masked_2d_impl_f64(
        data, mask, shape.ny, shape.nx,
    ))
}

pub fn apply_wrf_lake_interpolation_f32(
    data: &[f32],
    lu_index: &[f32],
    spec: WrfLakeMaskSpec,
) -> Result<Vec<f32>, PrepError> {
    let mask = wrf_small_water_mask(lu_index, spec)?;
    interpolate_masked_2d_f32(data, &mask, spec.shape)
}

pub fn apply_wrf_lake_interpolation_f64(
    data: &[f64],
    lu_index: &[f32],
    spec: WrfLakeMaskSpec,
) -> Result<Vec<f64>, PrepError> {
    let mask = wrf_small_water_mask(lu_index, spec)?;
    interpolate_masked_2d_f64(data, &mask, spec.shape)
}

fn validate_len(actual: usize, expected: usize) -> Result<(), PrepError> {
    if actual != expected {
        Err(PrepError::InvalidGridLength { expected, actual })
    } else {
        Ok(())
    }
}

fn interpolate_masked_2d_impl_f32(data: &[f32], mask: &[bool], ny: usize, nx: usize) -> Vec<f32> {
    let mut result = data.to_vec();
    let masked_indices: Vec<usize> = mask
        .iter()
        .enumerate()
        .filter_map(|(idx, &is_masked)| is_masked.then_some(idx))
        .collect();
    if masked_indices.is_empty() {
        return result;
    }

    let nearest_land_radius = nearest_land_radius(mask, ny, nx);
    if nearest_land_radius.is_empty() {
        return result;
    }

    let interpolated: Vec<(usize, f32)> = masked_indices
        .iter()
        .map(|&idx| {
            let cj = (idx / nx) as i32;
            let ci = (idx % nx) as i32;
            let radius = nearest_land_radius[idx];
            if radius == i32::MAX || radius <= 0 {
                return (idx, data[idx]);
            }

            let mut sum_val = 0.0f64;
            let mut sum_wt = 0.0f64;

            for dj in -radius..=radius {
                for di in -radius..=radius {
                    if dj.abs() != radius && di.abs() != radius {
                        continue;
                    }
                    let nj = cj + dj;
                    let ni = ci + di;
                    if nj < 0 || nj >= ny as i32 || ni < 0 || ni >= nx as i32 {
                        continue;
                    }
                    let nidx = nj as usize * nx + ni as usize;
                    if !mask[nidx] {
                        let dist = ((dj * dj + di * di) as f64).sqrt();
                        let weight = 1.0 / dist;
                        sum_val += f64::from(data[nidx]) * weight;
                        sum_wt += weight;
                    }
                }
            }

            if sum_wt > 0.0 {
                (idx, (sum_val / sum_wt) as f32)
            } else {
                (idx, data[idx])
            }
        })
        .collect();

    for (idx, value) in interpolated {
        result[idx] = value;
    }
    result
}

fn interpolate_masked_2d_impl_f64(data: &[f64], mask: &[bool], ny: usize, nx: usize) -> Vec<f64> {
    let mut result = data.to_vec();
    let masked_indices: Vec<usize> = mask
        .iter()
        .enumerate()
        .filter_map(|(idx, &is_masked)| is_masked.then_some(idx))
        .collect();
    if masked_indices.is_empty() {
        return result;
    }

    let nearest_land_radius = nearest_land_radius(mask, ny, nx);
    if nearest_land_radius.is_empty() {
        return result;
    }

    let interpolated: Vec<(usize, f64)> = masked_indices
        .iter()
        .map(|&idx| {
            let cj = (idx / nx) as i32;
            let ci = (idx % nx) as i32;
            let radius = nearest_land_radius[idx];
            if radius == i32::MAX || radius <= 0 {
                return (idx, data[idx]);
            }

            let mut sum_val = 0.0f64;
            let mut sum_wt = 0.0f64;

            for dj in -radius..=radius {
                for di in -radius..=radius {
                    if dj.abs() != radius && di.abs() != radius {
                        continue;
                    }
                    let nj = cj + dj;
                    let ni = ci + di;
                    if nj < 0 || nj >= ny as i32 || ni < 0 || ni >= nx as i32 {
                        continue;
                    }
                    let nidx = nj as usize * nx + ni as usize;
                    if !mask[nidx] {
                        let dist = ((dj * dj + di * di) as f64).sqrt();
                        let weight = 1.0 / dist;
                        sum_val += data[nidx] * weight;
                        sum_wt += weight;
                    }
                }
            }

            if sum_wt > 0.0 {
                (idx, sum_val / sum_wt)
            } else {
                (idx, data[idx])
            }
        })
        .collect();

    for (idx, value) in interpolated {
        result[idx] = value;
    }
    result
}

fn nearest_land_radius(mask: &[bool], ny: usize, nx: usize) -> Vec<i32> {
    let mut nearest_land_radius = vec![i32::MAX; mask.len()];
    let mut queue = VecDeque::new();

    for idx in 0..mask.len() {
        if !mask[idx] {
            nearest_land_radius[idx] = 0;
            queue.push_back(idx);
        }
    }

    if queue.is_empty() {
        return Vec::new();
    }

    while let Some(idx) = queue.pop_front() {
        let j = idx / nx;
        let i = idx % nx;
        let next_radius = nearest_land_radius[idx] + 1;

        for dj in -1i32..=1 {
            for di in -1i32..=1 {
                if dj == 0 && di == 0 {
                    continue;
                }
                let nj = j as i32 + dj;
                let ni = i as i32 + di;
                if nj < 0 || nj >= ny as i32 || ni < 0 || ni >= nx as i32 {
                    continue;
                }
                let nidx = nj as usize * nx + ni as usize;
                if next_radius < nearest_land_radius[nidx] {
                    nearest_land_radius[nidx] = next_radius;
                    queue.push_back(nidx);
                }
            }
        }
    }

    nearest_land_radius
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(nx: usize, ny: usize) -> GridShape {
        GridShape::new(nx, ny).unwrap()
    }

    #[test]
    fn wrf_small_water_mask_marks_only_small_connected_water() {
        let spec = WrfLakeMaskSpec::new(shape(5, 5), 1_000.0, 1_000.0, 4.1).unwrap();
        let mut lu = vec![1.0f32; 25];
        // 3-cell lake, diagonally connected
        lu[0] = 21.0;
        lu[1] = 21.0;
        lu[6] = 21.0;
        // 5-cell ocean-ish water body
        lu[18] = 16.0;
        lu[19] = 16.0;
        lu[23] = 17.0;
        lu[24] = 16.0;
        lu[14] = 16.0;

        let mask = wrf_small_water_mask(&lu, spec).unwrap();

        assert!(mask[0]);
        assert!(mask[1]);
        assert!(mask[6]);
        assert!(!mask[18]);
        assert!(!mask[19]);
        assert!(!mask[23]);
        assert!(!mask[24]);
        assert!(!mask[14]);
    }

    #[test]
    fn interpolate_masked_2d_f32_matches_ring_idw_behavior() {
        let shape = shape(3, 3);
        let data = vec![
            1.0, 2.0, 3.0, //
            4.0, 100.0, 6.0, //
            7.0, 8.0, 9.0,
        ];
        let mask = vec![
            false, false, false, //
            false, true, false, //
            false, false, false,
        ];

        let interpolated = interpolate_masked_2d_f32(&data, &mask, shape).unwrap();
        let expected = {
            let ring = [
                (-1i32, -1i32, 1.0f64),
                (-1, 0, 2.0),
                (-1, 1, 3.0),
                (0, -1, 4.0),
                (0, 1, 6.0),
                (1, -1, 7.0),
                (1, 0, 8.0),
                (1, 1, 9.0),
            ];
            let mut sum_val = 0.0;
            let mut sum_wt = 0.0;
            for (dj, di, value) in ring {
                let dist = ((dj * dj + di * di) as f64).sqrt();
                let weight = 1.0 / dist;
                sum_val += value * weight;
                sum_wt += weight;
            }
            (sum_val / sum_wt) as f32
        };

        assert!((interpolated[4] - expected).abs() < 1e-5);
        assert_eq!(interpolated[0], 1.0);
        assert_eq!(interpolated[8], 9.0);
    }

    #[test]
    fn apply_wrf_lake_interpolation_f64_leaves_all_water_grid_unchanged() {
        let shape = shape(2, 2);
        let spec = WrfLakeMaskSpec::new(shape, 1_000.0, 1_000.0, 10.0).unwrap();
        let lu = vec![21.0f32; 4];
        let data = vec![10.0f64, 20.0, 30.0, 40.0];

        let corrected = apply_wrf_lake_interpolation_f64(&data, &lu, spec).unwrap();

        assert_eq!(corrected, data);
    }

    #[test]
    fn validate_lengths() {
        let shape = shape(3, 2);
        let spec = WrfLakeMaskSpec::new(shape, 1_000.0, 1_000.0, 5.0).unwrap();
        let err = wrf_small_water_mask(&[21.0; 5], spec).unwrap_err();
        assert!(matches!(
            err,
            PrepError::InvalidGridLength {
                expected: 6,
                actual: 5
            }
        ));
    }
}
