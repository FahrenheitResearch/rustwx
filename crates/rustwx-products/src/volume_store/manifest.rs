use super::{VolumeResult, VolumeStoreError};
use crate::volume_store::grid::GridSpec;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeVariable {
    pub name: String,
    pub label: String,
    pub units: String,
}

impl VolumeVariable {
    pub fn new(
        name: impl Into<String>,
        label: impl Into<String>,
        units: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            units: units.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkShape {
    pub t: usize,
    pub z: usize,
    pub y: usize,
    pub x: usize,
}

impl ChunkShape {
    pub fn validate(self) -> VolumeResult<()> {
        if self.t == 0 || self.z == 0 || self.y == 0 || self.x == 0 {
            return Err(VolumeStoreError::InvalidManifest(
                "chunk dimensions must all be positive".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolumeManifest {
    pub format: String,
    pub model: String,
    pub domain: String,
    pub product: String,
    pub cycle: String,
    pub forecast_hours: Vec<u8>,
    pub variables: Vec<VolumeVariable>,
    pub levels_hpa: Vec<u16>,
    pub chunk_shape: ChunkShape,
    pub codec: String,
    pub grid: GridSpec,
}

impl VolumeManifest {
    pub fn validate(&self) -> VolumeResult<()> {
        if self.format != "rustwx-volume-store-v0" {
            return Err(VolumeStoreError::InvalidManifest(format!(
                "unsupported format '{}'",
                self.format
            )));
        }
        if self.variables.is_empty() {
            return Err(VolumeStoreError::InvalidManifest(
                "at least one variable is required".to_string(),
            ));
        }
        if self.forecast_hours.is_empty() {
            return Err(VolumeStoreError::InvalidManifest(
                "at least one forecast hour is required".to_string(),
            ));
        }
        if self.levels_hpa.is_empty() {
            return Err(VolumeStoreError::InvalidManifest(
                "at least one level is required".to_string(),
            ));
        }
        self.chunk_shape.validate()?;
        self.grid.validate()?;

        let mut variable_names = HashSet::new();
        for variable in &self.variables {
            if variable.name.trim().is_empty() {
                return Err(VolumeStoreError::InvalidManifest(
                    "variable names cannot be blank".to_string(),
                ));
            }
            if !variable_names.insert(variable.name.as_str()) {
                return Err(VolumeStoreError::InvalidManifest(format!(
                    "duplicate variable '{}'",
                    variable.name
                )));
            }
        }
        if self
            .forecast_hours
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(VolumeStoreError::InvalidManifest(
                "forecast hours must be strictly increasing".to_string(),
            ));
        }
        if self.levels_hpa.windows(2).any(|pair| pair[0] <= pair[1]) {
            return Err(VolumeStoreError::InvalidManifest(
                "pressure levels must be strictly descending".to_string(),
            ));
        }
        Ok(())
    }

    pub fn variable_index(&self, name: &str) -> VolumeResult<usize> {
        self.variables
            .iter()
            .position(|variable| variable.name == name)
            .ok_or_else(|| VolumeStoreError::MissingVariable(name.to_string()))
    }

    pub fn hour_index(&self, hour: u8) -> VolumeResult<usize> {
        self.forecast_hours
            .iter()
            .position(|candidate| *candidate == hour)
            .ok_or(VolumeStoreError::MissingHour(hour))
    }

    pub fn level_index(&self, level_hpa: u16) -> VolumeResult<usize> {
        self.levels_hpa
            .iter()
            .position(|candidate| *candidate == level_hpa)
            .ok_or(VolumeStoreError::MissingLevel(level_hpa))
    }

    pub fn block_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.variables.len(),
            self.forecast_hours.len().div_ceil(self.chunk_shape.t),
            self.levels_hpa.len().div_ceil(self.chunk_shape.z),
            self.grid.ny().div_ceil(self.chunk_shape.y)
                * self.grid.nx().div_ceil(self.chunk_shape.x),
        )
    }

    pub fn chunk_count(&self) -> usize {
        let nt = self.forecast_hours.len().div_ceil(self.chunk_shape.t);
        let nz = self.levels_hpa.len().div_ceil(self.chunk_shape.z);
        let ny = self.grid.ny().div_ceil(self.chunk_shape.y);
        let nx = self.grid.nx().div_ceil(self.chunk_shape.x);
        self.variables.len() * nt * nz * ny * nx
    }
}
