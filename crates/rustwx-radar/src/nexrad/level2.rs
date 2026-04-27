use anyhow::{Result, anyhow};
use byteorder::{BigEndian, ReadBytesExt};
use bzip2::read::BzDecoder;
#[cfg(not(target_arch = "wasm32"))]
use rayon::prelude::*;
use std::io::{Cursor, Read};

use chrono::{Datelike, NaiveDate};

use super::products::RadarProduct;

/// NEXRAD Level 2 (Archive II) file parser
/// Specification: ICD 2620010H (RDA/RPG)

const VOLUME_HEADER_SIZE: usize = 24;
const MSG_HEADER_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct Level2File {
    pub station_id: String,
    pub volume_date: u16,
    pub volume_time: u32,
    pub vcp: Option<u16>,
    pub sweeps: Vec<Level2Sweep>,
    /// True if parsing stopped early due to a read error (truncated volume).
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub struct Level2Sweep {
    pub elevation_number: u8,
    pub elevation_angle: f32,
    pub nyquist_velocity: Option<f32>, // m/s, from radial 'R' data block
    pub radials: Vec<RadialData>,
}

#[derive(Debug, Clone)]
pub struct RadialData {
    pub azimuth: f32,
    pub elevation: f32,
    pub azimuth_spacing: f32,
    pub nyquist_velocity: Option<f32>, // m/s, from radial 'R' data block
    /// Radial status: 0=start elev, 1=intermediate, 2=end elev, 3=start volume, 4=end volume.
    pub radial_status: u8,
    pub moments: Vec<MomentData>,
}

#[derive(Debug, Clone)]
pub struct MomentData {
    pub product: RadarProduct,
    pub gate_count: u16,
    pub first_gate_range: u16, // meters
    pub gate_size: u16,        // meters
    pub data: Vec<f32>,        // decoded values
}

#[derive(Debug)]
struct VolumeHeader {
    station_id: String,
    volume_date: u16,
    volume_time: u32,
}

#[derive(Debug)]
struct MessageHeader {
    message_size: u16,
    message_type: u8,
    _id_sequence: u16,
    _julian_date: u16,
    _milliseconds: u32,
    _segment_count: u16,
    _segment_number: u16,
}

#[derive(Debug)]
struct Message31Header {
    _radar_id: [u8; 4],
    azimuth_angle: f32,
    elevation_angle: f32,
    _azimuth_number: u16,
    elevation_number: u8,
    azimuth_resolution: u8,
    radial_status: u8,
    _cut_sector_number: u8,
    data_block_count: u16,
}

impl Level2File {
    pub fn parse(raw_data: &[u8]) -> Result<Self> {
        if raw_data.len() < VOLUME_HEADER_SIZE {
            return Err(anyhow!(
                "Data too short ({} bytes) — expected at least {} bytes for a NEXRAD Level 2 file",
                raw_data.len(),
                VOLUME_HEADER_SIZE
            ));
        }

        // Check if data starts with "AR2V" or "ARCHIVE2"
        let header_str = String::from_utf8_lossy(&raw_data[..std::cmp::min(9, raw_data.len())]);

        let data = if header_str.starts_with("AR2V") || header_str.starts_with("ARCH") {
            // Has volume header, decompress remainder
            Self::decompress_archive2(raw_data)?
        } else {
            // Already decompressed or different format
            raw_data.to_vec()
        };

        let mut cursor = Cursor::new(&data);

        // Parse volume header
        let header = Self::read_volume_header(&mut cursor)?;

        // Parse all messages
        // Parse messages into sweeps using radial_status to detect sweep
        // boundaries. This correctly separates SAILS/MRLE supplemental scans
        // that reuse the same elevation_number.
        let mut sweeps: Vec<Level2Sweep> = Vec::new();
        let mut current_sweep: Option<Level2Sweep> = None;
        let mut partial = false;
        let mut vcp: Option<u16> = None;

        while (cursor.position() as usize) < data.len() - MSG_HEADER_SIZE {
            match Self::read_message(&mut cursor, &data) {
                Ok(Some((elev_num, radial, msg_vcp))) => {
                    if vcp.is_none() {
                        vcp = msg_vcp;
                    }
                    // Start a new sweep on: radial_status 0 (start elev),
                    // 3 (start volume), 5 (start new elev same VCP),
                    // elevation_number change, or first radial.
                    let starts_new = radial.radial_status == 0
                        || radial.radial_status == 3
                        || radial.radial_status == 5
                        || current_sweep.is_none()
                        || current_sweep
                            .as_ref()
                            .map_or(false, |s| s.elevation_number != elev_num);

                    if starts_new {
                        if let Some(sweep) = current_sweep.take() {
                            if !sweep.radials.is_empty() {
                                sweeps.push(sweep);
                            }
                        }
                        current_sweep = Some(Level2Sweep {
                            elevation_number: elev_num,
                            elevation_angle: radial.elevation,
                            nyquist_velocity: radial.nyquist_velocity,
                            radials: vec![radial],
                        });
                    } else if let Some(ref mut sweep) = current_sweep {
                        if sweep.nyquist_velocity.is_none() && radial.nyquist_velocity.is_some() {
                            sweep.nyquist_velocity = radial.nyquist_velocity;
                        }
                        sweep.radials.push(radial);
                    }
                }
                Ok(None) => continue,
                Err(_) => {
                    partial = true;
                    break;
                }
            }
        }

        // Flush the last in-progress sweep
        if let Some(sweep) = current_sweep {
            if !sweep.radials.is_empty() {
                sweeps.push(sweep);
            }
        }

        // Sort sweeps by elevation angle for consistent ordering
        sweeps.sort_by(|a, b| {
            a.elevation_angle
                .partial_cmp(&b.elevation_angle)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(Level2File {
            station_id: header.station_id,
            volume_date: header.volume_date,
            volume_time: header.volume_time,
            vcp,
            sweeps,
            partial,
        })
    }

    /// Convert volume_date (modified Julian) and volume_time (ms since midnight)
    /// to a formatted UTC timestamp string like "2025-01-21 18:45:32 UTC".
    pub fn timestamp_string(&self) -> String {
        // NEXRAD modified Julian date: days since 1970-01-01 (epoch = day 1)
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let date = epoch + chrono::Duration::days((self.volume_date as i64) - 1);
        let total_secs = self.volume_time / 1000;
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
            date.year(),
            date.month(),
            date.day(),
            hours,
            minutes,
            seconds,
        )
    }

    pub fn available_products(&self) -> Vec<RadarProduct> {
        let mut products = std::collections::HashSet::new();
        for sweep in &self.sweeps {
            for radial in &sweep.radials {
                for moment in &radial.moments {
                    if moment.product != RadarProduct::Unknown {
                        products.insert(moment.product);
                    }
                }
            }
        }
        let mut products: Vec<_> = products.into_iter().collect();
        products.sort_by_key(|product| product.short_name().to_string());
        products
    }

    /// Return a human-readable description of the Volume Coverage Pattern.
    pub fn vcp_description(&self) -> Option<&'static str> {
        self.vcp.map(|v| match v {
            11 => "VCP 11 — Severe Weather (14 tilts, 5 min)",
            12 => "VCP 12 — Severe Weather (14 tilts, 4.1 min)",
            21 => "VCP 21 — Precipitation (9 tilts, 6 min)",
            32 => "VCP 32 — Clear Air Long Pulse (5 tilts, 10 min)",
            35 => "VCP 35 — Clear Air (5 tilts, 10 min)",
            112 => "VCP 112 — Severe Weather + SAILS (14 tilts)",
            121 => "VCP 121 — Precipitation + SAILS (9 tilts)",
            212 => "VCP 212 — Severe Weather + SAILS x2",
            215 => "VCP 215 — Severe Weather + MRLE (15 tilts)",
            221 => "VCP 221 — Precipitation + SAILS x2",
            _ => "VCP (unknown pattern)",
        })
    }

    /// Return Unix timestamp in milliseconds for this volume scan.
    pub fn unix_timestamp_ms(&self) -> i64 {
        let days_since_epoch = (self.volume_date as i64) - 1;
        let day_ms = days_since_epoch * 86_400_000;
        day_ms + self.volume_time as i64
    }

    fn decompress_archive2(raw_data: &[u8]) -> Result<Vec<u8>> {
        if raw_data.len() < VOLUME_HEADER_SIZE {
            return Err(anyhow!("Data too short for volume header"));
        }

        // Phase 1: collect block boundaries (fast, single-threaded scan)
        let mut blocks: Vec<(usize, usize, bool)> = Vec::new(); // (start, len, is_bz2)
        let mut pos = VOLUME_HEADER_SIZE;

        while pos < raw_data.len() {
            if pos + 4 > raw_data.len() {
                break;
            }

            let block_size = i32::from_be_bytes([
                raw_data[pos],
                raw_data[pos + 1],
                raw_data[pos + 2],
                raw_data[pos + 3],
            ]);
            pos += 4;

            let actual_size = block_size.unsigned_abs() as usize;
            if pos + actual_size > raw_data.len() {
                break;
            }

            let is_bz2 = actual_size >= 2 && raw_data[pos] == b'B' && raw_data[pos + 1] == b'Z';
            blocks.push((pos, actual_size, is_bz2));
            pos += actual_size;
        }

        // Phase 2: decompress all blocks in parallel (or sequentially on wasm)
        #[cfg(not(target_arch = "wasm32"))]
        let decompressed: Vec<Vec<u8>> = blocks
            .par_iter()
            .map(|&(start, len, is_bz2)| {
                let block_data = &raw_data[start..start + len];
                if is_bz2 {
                    let mut decoder = BzDecoder::new(block_data);
                    let mut out = Vec::new();
                    match decoder.read_to_end(&mut out) {
                        Ok(_) => out,
                        Err(_) => block_data.to_vec(),
                    }
                } else {
                    block_data.to_vec()
                }
            })
            .collect();
        #[cfg(target_arch = "wasm32")]
        let decompressed: Vec<Vec<u8>> = blocks
            .iter()
            .map(|&(start, len, is_bz2)| {
                let block_data = &raw_data[start..start + len];
                if is_bz2 {
                    let mut decoder = BzDecoder::new(block_data);
                    let mut out = Vec::new();
                    match decoder.read_to_end(&mut out) {
                        Ok(_) => out,
                        Err(_) => block_data.to_vec(),
                    }
                } else {
                    block_data.to_vec()
                }
            })
            .collect();

        // Phase 3: concatenate in order
        let total_size: usize =
            VOLUME_HEADER_SIZE + decompressed.iter().map(|b| b.len()).sum::<usize>();
        let mut result = Vec::with_capacity(total_size);
        result.extend_from_slice(&raw_data[..VOLUME_HEADER_SIZE]);
        for block in decompressed {
            result.extend_from_slice(&block);
        }

        Ok(result)
    }

    fn read_volume_header(cursor: &mut Cursor<&Vec<u8>>) -> Result<VolumeHeader> {
        // Archive II volume header is 24 bytes:
        //  0-11: filename (e.g. "AR2V0006.418")
        // 12-15: extension number (u32)
        // 16-17: date (modified Julian)
        // 18-21: time (ms since midnight, u32)
        // But actual ICAO is at bytes 20-23 in many files.
        // Safer: read the whole 24 bytes and extract ICAO from the raw data.
        let mut header = [0u8; 24];
        cursor.read_exact(&mut header)?;

        let filename_str = String::from_utf8_lossy(&header[..12]);

        // Try to get station from the last 4 bytes (ICAO)
        let icao = String::from_utf8_lossy(&header[20..24]).trim().to_string();

        // Fallback: try to get from the Message 31 headers later
        let station_id = if icao.len() == 4 && icao.chars().all(|c| c.is_ascii_alphanumeric()) {
            icao
        } else {
            // Try extracting from filename
            filename_str.chars().skip(4).take(4).collect::<String>()
        };

        // Archive II header: 12-15 = date (4 bytes, MJD in low 16 bits), 16-19 = time (ms)
        // The modified Julian date fits in u16 (~20500 for 2026). In the 4-byte big-endian
        // field, the high bytes are 0, so the date is in bytes 14-15.
        let volume_date = u16::from_be_bytes([header[14], header[15]]);
        let volume_time = u32::from_be_bytes([header[16], header[17], header[18], header[19]]);

        Ok(VolumeHeader {
            station_id,
            volume_date,
            volume_time,
        })
    }

    fn read_message(
        cursor: &mut Cursor<&Vec<u8>>,
        data: &[u8],
    ) -> Result<Option<(u8, RadialData, Option<u16>)>> {
        let start_pos = cursor.position() as usize;

        // CTM header (12 bytes) - skip
        if start_pos + 12 > data.len() {
            return Err(anyhow!("End of data"));
        }

        // Check for CTM header
        let mut ctm = [0u8; 12];
        cursor.read_exact(&mut ctm)?;

        // Read message header
        if (cursor.position() as usize) + MSG_HEADER_SIZE > data.len() {
            return Err(anyhow!("End of data"));
        }

        let msg_header = Self::read_message_header(cursor)?;

        // We only care about Message Type 31 (Digital Radar Data)
        if msg_header.message_type != 31 {
            // Skip to next message (messages are 2432 bytes aligned for legacy types)
            let next_pos = start_pos + 2432;
            if next_pos <= data.len() {
                cursor.set_position(next_pos as u64);
            } else {
                return Err(anyhow!("End of data"));
            }
            return Ok(None);
        }

        // Parse Message 31
        let msg31_start = cursor.position() as usize;
        let msg31 = Self::read_msg31_header(cursor)?;

        // Read data block pointers
        let mut block_pointers = Vec::new();
        for _ in 0..msg31.data_block_count {
            let offset = cursor.read_u32::<BigEndian>()?;
            block_pointers.push(offset);
        }

        // Parse each data block
        let mut moments = Vec::new();
        let mut nyquist_velocity: Option<f32> = None;
        let mut vcp: Option<u16> = None;

        for ptr_offset in &block_pointers {
            let block_pos = msg31_start + *ptr_offset as usize;
            if block_pos + 4 > data.len() {
                continue;
            }

            let block_type = data[block_pos];

            // 'D' = data moment block
            if block_type == b'D' {
                if let Ok(moment) = Self::parse_moment_block(data, block_pos) {
                    // Filter out unknown moments (e.g. "CFP") to prevent
                    // polluting downstream product lookups
                    if moment.product != RadarProduct::Unknown {
                        moments.push(moment);
                    }
                }
            }
            // 'R' = radial data block (contains Nyquist velocity)
            else if block_type == b'R' {
                nyquist_velocity = Self::parse_radial_block_nyquist(data, block_pos);
            }
            // 'V' = volume data block (contains VCP number)
            else if block_type == b'V' {
                vcp = Self::parse_volume_block_vcp(data, block_pos);
            }
        }

        // Calculate next message position
        let msg_size_bytes = (msg_header.message_size as usize) * 2 + 12; // +12 for CTM
        let next_pos = start_pos + std::cmp::max(msg_size_bytes, 2432);
        if next_pos <= data.len() {
            cursor.set_position(next_pos as u64);
        }

        let radial = RadialData {
            azimuth: msg31.azimuth_angle,
            elevation: msg31.elevation_angle,
            azimuth_spacing: if msg31.azimuth_resolution == 1 {
                0.5
            } else {
                1.0
            },
            nyquist_velocity,
            radial_status: msg31.radial_status,
            moments,
        };

        Ok(Some((msg31.elevation_number, radial, vcp)))
    }

    fn read_message_header(cursor: &mut Cursor<&Vec<u8>>) -> Result<MessageHeader> {
        let message_size = cursor.read_u16::<BigEndian>()?;
        let _rda_channel = cursor.read_u8()?;
        let message_type = cursor.read_u8()?;
        let id_sequence = cursor.read_u16::<BigEndian>()?;
        let julian_date = cursor.read_u16::<BigEndian>()?;
        let milliseconds = cursor.read_u32::<BigEndian>()?;
        let segment_count = cursor.read_u16::<BigEndian>()?;
        let segment_number = cursor.read_u16::<BigEndian>()?;

        Ok(MessageHeader {
            message_size,
            message_type,
            _id_sequence: id_sequence,
            _julian_date: julian_date,
            _milliseconds: milliseconds,
            _segment_count: segment_count,
            _segment_number: segment_number,
        })
    }

    fn read_msg31_header(cursor: &mut Cursor<&Vec<u8>>) -> Result<Message31Header> {
        let mut radar_id = [0u8; 4];
        cursor.read_exact(&mut radar_id)?;

        let _collection_time = cursor.read_u32::<BigEndian>()?;
        let _collection_date = cursor.read_u16::<BigEndian>()?;
        let azimuth_number = cursor.read_u16::<BigEndian>()?;
        let azimuth_angle = cursor.read_f32::<BigEndian>()?;
        let _compression_indicator = cursor.read_u8()?;
        let _spare = cursor.read_u8()?;
        let _radial_length = cursor.read_u16::<BigEndian>()?;
        let azimuth_resolution = cursor.read_u8()?;
        let radial_status = cursor.read_u8()?;
        let elevation_number = cursor.read_u8()?;
        let cut_sector_number = cursor.read_u8()?;
        let elevation_angle = cursor.read_f32::<BigEndian>()?;
        let _radial_spot_blanking = cursor.read_u8()?;
        let _azimuth_indexing_mode = cursor.read_u8()?;
        let data_block_count = cursor.read_u16::<BigEndian>()?;

        Ok(Message31Header {
            _radar_id: radar_id,
            azimuth_angle,
            elevation_angle,
            _azimuth_number: azimuth_number,
            elevation_number,
            azimuth_resolution,
            radial_status,
            _cut_sector_number: cut_sector_number,
            data_block_count,
        })
    }

    /// Parse a Radial Data ('R') block to extract the Nyquist velocity.
    /// ICD 2620010H Table XVII-B: Radial Data block is at least 28 bytes.
    /// Byte 0: 'R', Bytes 1-3: "RAD"
    /// Bytes 16-17: Unambiguous range (km/10, u16)
    /// Bytes 26-27: Nyquist velocity (m/s * 100, u16) — note: some docs say offset 28
    fn parse_radial_block_nyquist(data: &[u8], offset: usize) -> Option<f32> {
        // The 'R' block needs at least 28 bytes
        if offset + 28 > data.len() {
            return None;
        }
        // Nyquist velocity is at bytes 26-27 in the radial block (u16, scaled by 100)
        let nyquist_raw = u16::from_be_bytes([data[offset + 26], data[offset + 27]]);
        if nyquist_raw == 0 {
            return None;
        }
        Some(nyquist_raw as f32 / 100.0)
    }

    /// Parse a Volume Data ('V') block to extract the VCP number.
    /// ICD 2620010H Table XVII-A: Volume Data block.
    /// Byte 0: 'V', Bytes 1-3: "VOL"
    /// Bytes 40-41: Volume Coverage Pattern number (u16)
    fn parse_volume_block_vcp(data: &[u8], offset: usize) -> Option<u16> {
        // The 'V' block needs at least 44 bytes to reach VCP at offset 40
        if offset + 44 > data.len() {
            return None;
        }
        // Verify block identifier
        if data.get(offset + 1..offset + 4) != Some(b"VOL") {
            return None;
        }
        let vcp = u16::from_be_bytes([data[offset + 40], data[offset + 41]]);
        if vcp == 0 {
            return None;
        }
        Some(vcp)
    }

    fn parse_moment_block(data: &[u8], offset: usize) -> Result<MomentData> {
        if offset + 28 > data.len() {
            return Err(anyhow!("Moment block too short"));
        }

        let mut cursor = Cursor::new(&data[offset..]);

        // Data moment block header (ICD 2620010H Table XVII-E):
        // Byte 0: Data block type ('D')
        // Bytes 1-3: Moment name (e.g. "REF", "VEL", "SW ", "ZDR", "PHI", "RHO", "CFP")
        let _block_type = cursor.read_u8()?;
        let mut name_bytes = [0u8; 3];
        cursor.read_exact(&mut name_bytes)?;
        let name = String::from_utf8_lossy(&name_bytes).trim().to_string();

        // Bytes 4-7: Reserved (u32)
        let _reserved = cursor.read_u32::<BigEndian>()?;
        // Bytes 8-9: Number of data moment gates (u16)
        let gate_count = cursor.read_u16::<BigEndian>()?;
        // Bytes 10-11: Range to center of first gate (m) (u16)
        let first_gate_range = cursor.read_u16::<BigEndian>()?;
        // Bytes 12-13: Data moment gate interval (m) (u16)
        let gate_size = cursor.read_u16::<BigEndian>()?;
        // Bytes 14-15: Tover / SNR threshold parameter (u16)
        let _tover = cursor.read_u16::<BigEndian>()?;
        // Byte 16: SNR threshold (u8)
        let _snr_threshold = cursor.read_u8()?;
        // Byte 17: Control flags (u8)
        let _control_flags = cursor.read_u8()?;
        // Bytes 18-19: Data word size in bits (u16) - 8 or 16
        let data_word_size = cursor.read_u16::<BigEndian>()?;
        // Bytes 20-23: Scale (f32)
        let scale = cursor.read_f32::<BigEndian>()?;
        // Bytes 24-27: Offset (f32)
        let offset_val = cursor.read_f32::<BigEndian>()?;

        let product = RadarProduct::from_name(&name);

        // Guard against invalid scale (would produce inf/NaN values)
        if scale == 0.0 {
            return Err(anyhow!("Moment block '{}' has zero scale", name));
        }

        // Read gate data in bulk — avoids per-gate Cursor overhead.
        // Precompute 1/scale for multiplication instead of division in the hot loop.
        let inv_scale = 1.0 / scale;
        let gate_count_usize = gate_count as usize;
        let data_start = cursor.position() as usize;
        let raw_slice = &data[offset..];

        let mut decoded = Vec::with_capacity(gate_count_usize);

        if data_word_size >= 16 {
            // 16-bit gates: read 2 bytes per gate directly from the slice
            let byte_len = gate_count_usize * 2;
            let gate_bytes =
                &raw_slice[data_start..data_start + byte_len.min(raw_slice.len() - data_start)];
            for chunk in gate_bytes.chunks_exact(2) {
                let raw = u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
                let value = if raw <= 1 {
                    f32::NAN
                } else {
                    (raw as f32 - offset_val) * inv_scale
                };
                decoded.push(value);
            }
        } else {
            // 8-bit gates: read 1 byte per gate directly from the slice
            let gate_bytes = &raw_slice
                [data_start..data_start + gate_count_usize.min(raw_slice.len() - data_start)];
            for &byte in gate_bytes {
                let raw = byte as u32;
                let value = if raw <= 1 {
                    f32::NAN
                } else {
                    (raw as f32 - offset_val) * inv_scale
                };
                decoded.push(value);
            }
        }

        Ok(MomentData {
            product,
            gate_count,
            first_gate_range,
            gate_size,
            data: decoded,
        })
    }
}
