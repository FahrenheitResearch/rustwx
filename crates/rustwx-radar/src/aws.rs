use std::io::Read;

use chrono::{Datelike, NaiveDate, Utc};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};

const NEXRAD_BASE_URL: &str = "https://unidata-nexrad-level2.s3.amazonaws.com";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NexradObject {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct NexradDownload {
    pub object: NexradObject,
    pub bytes: Vec<u8>,
}

pub fn list_day(site: &str, date: NaiveDate) -> anyhow::Result<Vec<NexradObject>> {
    let agent = build_agent();
    let prefix = format!(
        "{:04}/{:02}/{:02}/{}/",
        date.year(),
        date.month(),
        date.day(),
        site.to_uppercase()
    );
    let url = format!("{NEXRAD_BASE_URL}?list-type=2&prefix={prefix}");
    let mut response = agent.get(&url).call()?;
    let xml = response.body_mut().read_to_string()?;
    let mut objects = parse_s3_list_xml(&xml);
    objects.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(objects)
}

pub fn fetch_object(key: &str) -> anyhow::Result<Vec<u8>> {
    let agent = build_agent();
    let url = format!("{NEXRAD_BASE_URL}/{key}");
    let mut response = agent.get(&url).call()?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(200 * 1024 * 1024)
        .read_to_vec()?;
    Ok(maybe_decompress_gzip(bytes))
}

pub fn fetch_latest(site: &str) -> anyhow::Result<NexradDownload> {
    let today = Utc::now().date_naive();
    let yesterday = today - chrono::Duration::days(1);
    for date in [today, yesterday] {
        let objects = list_day(site, date)?;
        if let Some(object) = objects.last().cloned() {
            let bytes = fetch_object(&object.key)?;
            return Ok(NexradDownload { object, bytes });
        }
    }
    anyhow::bail!("no public NEXRAD Level-II files found for {site} today or yesterday")
}

fn parse_s3_list_xml(xml: &str) -> Vec<NexradObject> {
    let mut objects = Vec::new();
    for contents in xml.split("<Contents>").skip(1) {
        let end = contents.find("</Contents>").unwrap_or(contents.len());
        let block = &contents[..end];
        let key = extract_xml_tag(block, "Key").unwrap_or_default();
        let display_name = key.rsplit('/').next().unwrap_or(&key).to_string();
        if key.is_empty() || display_name.ends_with("_MDM") || display_name.ends_with(".md") {
            continue;
        }
        let size = extract_xml_tag(block, "Size")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let last_modified = extract_xml_tag(block, "LastModified").unwrap_or_default();
        objects.push(NexradObject {
            key,
            size,
            last_modified,
            display_name,
        });
    }
    objects
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

fn maybe_decompress_gzip(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() < 2 || bytes[0] != 0x1f || bytes[1] != 0x8b {
        return bytes;
    }
    let mut decoder = GzDecoder::new(&bytes[..]);
    let mut out = Vec::new();
    match decoder.read_to_end(&mut out) {
        Ok(_) if !out.is_empty() => out,
        _ => bytes,
    }
}

fn build_agent() -> ureq::Agent {
    rustls::crypto::CryptoProvider::install_default(rustls_rustcrypto::provider()).ok();
    let crypto = std::sync::Arc::new(rustls_rustcrypto::provider());
    ureq::Agent::config_builder()
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .provider(ureq::tls::TlsProvider::Rustls)
                .root_certs(ureq::tls::RootCerts::WebPki)
                .unversioned_rustls_crypto_provider(crypto)
                .build(),
        )
        .build()
        .new_agent()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_objects() {
        let xml = r#"
        <ListBucketResult>
          <Contents>
            <Key>2026/04/26/KTLX/KTLX20260426_120000_V06</Key>
            <LastModified>2026-04-26T12:01:00.000Z</LastModified>
            <Size>123</Size>
          </Contents>
          <Contents>
            <Key>2026/04/26/KTLX/KTLX20260426_120000_V06_MDM</Key>
            <LastModified>2026-04-26T12:01:01.000Z</LastModified>
            <Size>12</Size>
          </Contents>
        </ListBucketResult>
        "#;
        let objects = parse_s3_list_xml(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].display_name, "KTLX20260426_120000_V06");
    }
}
