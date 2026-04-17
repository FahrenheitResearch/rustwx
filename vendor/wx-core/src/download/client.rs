use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use rayon::prelude::*;
use ureq::http::header::LOCATION;

use super::cache::DiskCache;

/// HTTP client for downloading GRIB2 data with byte-range support.
///
/// Uses ureq (blocking HTTP) with rustls + rustcrypto for TLS.
/// Supports configurable timeouts, retry with exponential backoff,
/// parallel chunk downloads, and optional disk caching.
pub struct DownloadClient {
    agent: ureq::Agent,
    #[allow(dead_code)]
    timeout: Duration,
    max_retries: u32,
    cache: Option<DiskCache>,
}

/// Maximum body size for full file downloads.
///
/// Full HRRR/RRFS family files can exceed the older subset-oriented 500 MB cap,
/// especially `wrfnat`. Keep the cap comfortably above current operational
/// artifacts while still guarding against obviously runaway downloads.
const MAX_BODY_SIZE: u64 = 8 * 1024 * 1024 * 1024;

/// Default timeout per request.
///
/// Full-family GRIB downloads routinely take longer than the old 30 s subset
/// budget, especially from NOMADS. Use a longer default so whole-file ingest is
/// viable without custom client wiring.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Default maximum number of retries.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Maximum redirects we will follow manually.
///
/// NOMADS file URLs should generally be direct. We disable ureq's built-in
/// redirect handling so malformed upstream 3xx responses do not bubble up as
/// opaque protocol errors such as "missing a location header", then follow
/// only well-formed redirects ourselves.
const MAX_REDIRECTS: u32 = 10;

/// Backoff durations for each retry attempt.
const BACKOFF_DURATIONS: [Duration; 3] = [
    Duration::from_millis(500),
    Duration::from_millis(1000),
    Duration::from_millis(2000),
];

/// Longer backoff for the Akamai "Over Rate Limit" behavior seen on NOMADS.
const NOMADS_RATE_LIMIT_BACKOFF_DURATIONS: [Duration; 3] = [
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(20),
];

/// Minimum spacing between NOMADS requests from this process.
const NOMADS_MIN_REQUEST_GAP: Duration = Duration::from_millis(750);

static NOMADS_REQUEST_GATE: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();

/// Configuration for creating a DownloadClient.
pub struct DownloadConfig {
    /// Timeout per HTTP request.
    pub timeout: Duration,
    /// Maximum number of retry attempts.
    pub max_retries: u32,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

/// Check whether an error from ureq should be retried.
///
/// Retries on: connection/transport errors, 429 (rate limit),
/// 500, 502, 503, 504 (server errors).
/// Does NOT retry on: 400, 404, or other 4xx client errors.
fn is_retryable(err: &ureq::Error) -> bool {
    match err {
        ureq::Error::StatusCode(code) => {
            let c = *code;
            c == 429 || c == 500 || c == 502 || c == 503 || c == 504
        }
        // Timeout, DNS, connection reset, etc. — all retryable.
        _ => true,
    }
}

fn is_nomads_url(url: &str) -> bool {
    url.contains("nomads.ncep.noaa.gov")
}

fn is_probable_nomads_rate_limit(url: &str, err: &ureq::Error) -> bool {
    is_nomads_url(url) && err.to_string().contains("missing a location header")
}

fn is_redirect_status(status: ureq::http::StatusCode) -> bool {
    status.is_redirection()
}

fn resolve_redirect_url(current_url: &str, location: &str) -> crate::error::Result<String> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return Ok(location.to_string());
    }

    let current_uri: ureq::http::Uri = current_url.parse().map_err(|err| {
        crate::RustmetError::Http(format!(
            "failed to parse redirect source URL {}: {}",
            current_url, err
        ))
    })?;

    let scheme = current_uri.scheme_str().ok_or_else(|| {
        crate::RustmetError::Http(format!(
            "redirect source URL {} is missing a scheme",
            current_url
        ))
    })?;
    let authority = current_uri.authority().ok_or_else(|| {
        crate::RustmetError::Http(format!(
            "redirect source URL {} is missing an authority",
            current_url
        ))
    })?;

    if location.starts_with('/') {
        return Ok(format!("{}://{}{}", scheme, authority, location));
    }

    let path = current_uri.path();
    let directory = path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    let joined = if directory.is_empty() {
        format!("/{}", location)
    } else {
        format!("{}/{}", directory, location)
    };
    Ok(format!("{}://{}{}", scheme, authority, joined))
}

fn pace_request(url: &str) {
    if !is_nomads_url(url) {
        return;
    }

    let gate = NOMADS_REQUEST_GATE.get_or_init(|| Mutex::new(None));
    let mut last = gate.lock().expect("nomads request gate poisoned");
    if let Some(previous) = *last {
        let elapsed = previous.elapsed();
        if elapsed < NOMADS_MIN_REQUEST_GAP {
            std::thread::sleep(NOMADS_MIN_REQUEST_GAP - elapsed);
        }
    }
    *last = Some(Instant::now());
}

/// Build a ureq agent with TLS configured via rustls-rustcrypto.
fn build_agent(config: &DownloadConfig) -> ureq::Agent {
    // Install the rustcrypto provider as the process-wide default.
    rustls::crypto::CryptoProvider::install_default(rustls_rustcrypto::provider()).ok();

    let crypto = Arc::new(rustls_rustcrypto::provider());

    ureq::Agent::config_builder()
        .tls_config(
            ureq::tls::TlsConfig::builder()
                .provider(ureq::tls::TlsProvider::Rustls)
                .root_certs(ureq::tls::RootCerts::WebPki)
                .unversioned_rustls_crypto_provider(crypto)
                .build(),
        )
        .max_redirects(0)
        .timeout_global(Some(config.timeout))
        .build()
        .new_agent()
}

impl DownloadClient {
    fn perform_get(
        &self,
        url: &str,
        range_header: Option<&str>,
    ) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
        let mut request = self.agent.get(url);
        if let Some(range_header) = range_header {
            request = request.header("Range", range_header);
        }
        request.call()
    }

    fn get_response_following_redirects(
        &self,
        url: &str,
        range_header: Option<&str>,
    ) -> crate::error::Result<ureq::http::Response<ureq::Body>> {
        let mut current_url = url.to_string();
        let mut malformed_redirect_retries = 0u32;

        for redirect_count in 0..=MAX_REDIRECTS {
            let request_url = current_url.clone();
            let response = self.with_retry(&request_url, || {
                self.perform_get(&request_url, range_header)
            })?;
            let status = response.status();

            if is_redirect_status(status) {
                if redirect_count == MAX_REDIRECTS {
                    return Err(crate::RustmetError::Http(format!(
                        "too many redirects while requesting {}",
                        url
                    )));
                }

                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok());

                let Some(location) = location else {
                    if is_nomads_url(&request_url) && malformed_redirect_retries < self.max_retries
                    {
                        let backoff = NOMADS_RATE_LIMIT_BACKOFF_DURATIONS
                            .get(malformed_redirect_retries as usize)
                            .copied()
                            .unwrap_or(
                                NOMADS_RATE_LIMIT_BACKOFF_DURATIONS
                                    [NOMADS_RATE_LIMIT_BACKOFF_DURATIONS.len() - 1],
                            );
                        malformed_redirect_retries += 1;
                        eprintln!(
                            "  Retry {}/{} for {} after {:?} (probable NOMADS malformed redirect {})",
                            malformed_redirect_retries,
                            self.max_retries,
                            request_url,
                            backoff,
                            status
                        );
                        std::thread::sleep(backoff);
                        continue;
                    }

                    return Err(crate::RustmetError::Http(format!(
                        "redirect response missing Location header for {} (status {})",
                        request_url, status
                    )));
                };

                current_url = resolve_redirect_url(&request_url, location)?;
                continue;
            }

            return Ok(response);
        }

        Err(crate::RustmetError::Http(format!(
            "too many redirects while requesting {}",
            url
        )))
    }

    fn probe_nomads_range_ok(&self, url: &str) -> bool {
        for attempt in 0..=1u32 {
            let mut current_url = url.to_string();
            let mut retry = false;

            for _ in 0..=MAX_REDIRECTS {
                pace_request(&current_url);
                match self.perform_get(&current_url, Some("bytes=0-0")) {
                    Ok(response) => {
                        let status = response.status();
                        if is_redirect_status(status) {
                            let Some(location) = response
                                .headers()
                                .get(LOCATION)
                                .and_then(|value| value.to_str().ok())
                            else {
                                retry = attempt == 0;
                                break;
                            };

                            let Ok(next_url) = resolve_redirect_url(&current_url, location) else {
                                retry = attempt == 0;
                                break;
                            };
                            current_url = next_url;
                            continue;
                        }

                        return true;
                    }
                    Err(ureq::Error::StatusCode(code)) if code == 404 || code == 403 => {
                        return false;
                    }
                    Err(err) => {
                        retry = attempt == 0 && is_retryable(&err);
                        break;
                    }
                }
            }

            if retry {
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
            return false;
        }

        false
    }

    /// Create a new download client with TLS configured via rustls-rustcrypto.
    ///
    /// Uses ureq's built-in TlsConfig with the rustcrypto provider and
    /// webpki root certificates (Mozilla's CA bundle). No caching.
    pub fn new() -> crate::error::Result<Self> {
        Self::new_with_config(DownloadConfig::default())
    }

    /// Create a new download client with custom timeout and retry settings.
    /// No caching.
    pub fn new_with_config(config: DownloadConfig) -> crate::error::Result<Self> {
        let agent = build_agent(&config);
        Ok(Self {
            agent,
            timeout: config.timeout,
            max_retries: config.max_retries,
            cache: None,
        })
    }

    /// Create a new download client with disk caching enabled.
    ///
    /// If `cache_dir` is `Some`, files are cached there. If `None`, the
    /// platform default is used (`~/.cache/metrust/` on Linux/macOS,
    /// `%LOCALAPPDATA%/metrust/cache/` on Windows).
    pub fn new_with_cache(cache_dir: Option<&str>) -> crate::error::Result<Self> {
        let config = DownloadConfig::default();
        let agent = build_agent(&config);
        let cache = match cache_dir {
            Some(dir) => DiskCache::with_dir(std::path::PathBuf::from(dir)),
            None => DiskCache::new(),
        };
        Ok(Self {
            agent,
            timeout: config.timeout,
            max_retries: config.max_retries,
            cache: Some(cache),
        })
    }

    /// Attach a `DiskCache` to this client. Replaces any existing cache.
    pub fn set_cache(&mut self, cache: DiskCache) {
        self.cache = Some(cache);
    }

    /// Return a reference to the underlying HTTP agent.
    ///
    /// Used by the streaming download module to make requests with
    /// manual body reading.
    pub fn agent(&self) -> &ureq::Agent {
        &self.agent
    }

    /// Return a reference to the cache, if one is attached.
    pub fn cache(&self) -> Option<&DiskCache> {
        self.cache.as_ref()
    }

    /// Execute a request-producing closure with retry and exponential backoff.
    ///
    /// `attempt_fn` is called on each attempt and must produce the final result
    /// or a ureq::Error. This avoids needing to name the ureq Response type.
    fn with_retry<T, F>(&self, url: &str, attempt_fn: F) -> crate::error::Result<T>
    where
        F: Fn() -> Result<T, ureq::Error>,
    {
        let mut last_err = String::new();

        for attempt in 0..=self.max_retries {
            pace_request(url);
            match attempt_fn() {
                Ok(val) => return Ok(val),
                Err(e) => {
                    let probable_nomads_rate_limit = is_probable_nomads_rate_limit(url, &e);
                    last_err = if probable_nomads_rate_limit {
                        format!("probable NOMADS rate-limit response for {}: {}", url, e)
                    } else {
                        format!("{}", e)
                    };

                    if attempt < self.max_retries && is_retryable(&e) {
                        let backoff = if probable_nomads_rate_limit {
                            NOMADS_RATE_LIMIT_BACKOFF_DURATIONS
                                .get(attempt as usize)
                                .copied()
                                .unwrap_or(
                                    NOMADS_RATE_LIMIT_BACKOFF_DURATIONS
                                        [NOMADS_RATE_LIMIT_BACKOFF_DURATIONS.len() - 1],
                                )
                        } else {
                            BACKOFF_DURATIONS
                                .get(attempt as usize)
                                .copied()
                                .unwrap_or(BACKOFF_DURATIONS[BACKOFF_DURATIONS.len() - 1])
                        };
                        eprintln!(
                            "  Retry {}/{} for {} after {:?} ({})",
                            attempt + 1,
                            self.max_retries,
                            url,
                            backoff,
                            e
                        );
                        std::thread::sleep(backoff);
                    } else {
                        break;
                    }
                }
            }
        }

        Err(crate::RustmetError::Http(format!(
            "HTTP request failed for {}: {}",
            url, last_err
        )))
    }

    /// Send a HEAD request and return true if the server responds with 200 OK.
    ///
    /// Does NOT retry on 404 — only retries on transient/server errors.
    /// Useful for probing whether a remote file exists (e.g., .idx files).
    pub fn head_ok(&self, url: &str) -> bool {
        if is_nomads_url(url) {
            return self.probe_nomads_range_ok(url);
        }

        // Single attempt with one retry on transient errors.
        for attempt in 0..=1u32 {
            match self.agent.head(url).call() {
                Ok(_) => return true,
                Err(ureq::Error::StatusCode(code)) if code == 404 || code == 403 => {
                    return false;
                }
                Err(e) => {
                    if attempt == 0 && is_retryable(&e) {
                        std::thread::sleep(std::time::Duration::from_millis(300));
                        continue;
                    }
                    return false;
                }
            }
        }
        false
    }

    /// Download a full URL and return the response body as bytes.
    ///
    /// If caching is enabled, checks cache first and stores the result after
    /// a successful download. Cache failures are silently ignored.
    pub fn get_bytes(&self, url: &str) -> crate::error::Result<Vec<u8>> {
        let key = DiskCache::cache_key(url, None);

        // Try cache first
        if let Some(cache) = &self.cache {
            if let Some(data) = cache.get(&key) {
                return Ok(data);
            }
        }

        let mut response = self.get_response_following_redirects(url, None)?;
        let data = response
            .body_mut()
            .with_config()
            .limit(MAX_BODY_SIZE)
            .read_to_vec()
            .map_err(|err| crate::RustmetError::Http(format!("failed to read {}: {}", url, err)))?;

        // Store in cache (errors silently ignored)
        if let Some(cache) = &self.cache {
            cache.put(&key, &data);
        }

        Ok(data)
    }

    /// Download a URL and return the response body as a string (for .idx files).
    ///
    /// Text responses (like .idx) are NOT cached because they are small and
    /// may change between model runs.
    pub fn get_text(&self, url: &str) -> crate::error::Result<String> {
        let mut response = self.get_response_following_redirects(url, None)?;
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|err| crate::RustmetError::Http(format!("failed to read {}: {}", url, err)))?;
        Ok(text)
    }

    /// Download a specific byte range from a URL.
    ///
    /// If caching is enabled, the result is keyed by URL + byte range.
    /// Cache failures are silently ignored.
    pub fn get_range(&self, url: &str, start: u64, end: u64) -> crate::error::Result<Vec<u8>> {
        let key = DiskCache::cache_key(url, Some((start, end)));

        // Try cache first
        if let Some(cache) = &self.cache {
            if let Some(data) = cache.get(&key) {
                return Ok(data);
            }
        }

        let range_header = if end == u64::MAX {
            format!("bytes={}-", start)
        } else {
            format!("bytes={}-{}", start, end)
        };

        let mut response = self.get_response_following_redirects(url, Some(&range_header))?;
        let data = response
            .body_mut()
            .with_config()
            .limit(MAX_BODY_SIZE)
            .read_to_vec()
            .map_err(|err| crate::RustmetError::Http(format!("failed to read {}: {}", url, err)))?;

        // Store in cache (errors silently ignored)
        if let Some(cache) = &self.cache {
            cache.put(&key, &data);
        }

        Ok(data)
    }

    /// Download multiple byte ranges from a URL in parallel and concatenate the results.
    ///
    /// Each range is downloaded as a separate HTTP request with a Range header.
    /// Uses rayon to download chunks concurrently. Progress is printed to stderr.
    ///
    /// If caching is enabled, the combined result is cached under a key derived
    /// from the URL and all ranges. Individual ranges are also cached by
    /// `get_range`, so partial overlaps with future requests benefit from the
    /// cache too.
    pub fn get_ranges(&self, url: &str, ranges: &[(u64, u64)]) -> crate::error::Result<Vec<u8>> {
        let total = ranges.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        // Check for the combined result in cache
        let combined_key = DiskCache::cache_key_ranges(url, ranges);
        if let Some(cache) = &self.cache {
            if let Some(data) = cache.get(&combined_key) {
                return Ok(data);
            }
        }

        let completed = AtomicUsize::new(0);

        let results: Vec<crate::error::Result<Vec<u8>>> = if is_nomads_url(url) {
            ranges
                .iter()
                .map(|&(start, end)| {
                    let data = self.get_range(url, start, end)?;
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    eprint!("\r  Downloading chunks {}/{}...", done, total);
                    Ok(data)
                })
                .collect()
        } else {
            // Download all chunks in parallel, preserving order.
            // Each chunk is individually cached via get_range.
            ranges
                .par_iter()
                .map(|&(start, end)| {
                    let data = self.get_range(url, start, end)?;
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    eprint!("\r  Downloading chunks {}/{}...", done, total);
                    Ok(data)
                })
                .collect()
        };

        // Concatenate results in order, propagating the first error.
        let mut combined = Vec::new();
        for result in results {
            combined.extend_from_slice(&result?);
        }

        eprintln!(
            "\r  Downloaded {} chunks, {} bytes total.    ",
            total,
            combined.len()
        );

        // Cache the combined result (errors silently ignored)
        if let Some(cache) = &self.cache {
            cache.put(&combined_key, &combined);
        }

        Ok(combined)
    }
}

#[cfg(test)]
mod tests {
    use super::{DownloadClient, DownloadConfig};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn spawn_http_server(responses: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("server addr");
        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept connection");
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                stream.write_all(&response).expect("write response");
                stream.flush().expect("flush response");
            }
        });
        format!("http://{}", addr)
    }

    fn test_client() -> DownloadClient {
        DownloadClient::new_with_config(DownloadConfig {
            timeout: Duration::from_secs(5),
            max_retries: 1,
        })
        .expect("client")
    }

    #[test]
    fn get_bytes_follows_relative_redirects() {
        let base = spawn_http_server(vec![
            b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello".to_vec(),
        ]);
        let client = test_client();
        let body = client
            .get_bytes(&format!("{}/start", base))
            .expect("redirected body");
        assert_eq!(body, b"hello");
    }

    #[test]
    fn get_bytes_surfaces_clear_error_for_redirect_without_location() {
        let base = spawn_http_server(vec![
            b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        ]);
        let client = test_client();
        let err = client
            .get_bytes(&format!("{}/broken", base))
            .expect_err("missing location should fail");
        let message = err.to_string();
        assert!(message.contains("redirect response missing Location header"));
        assert!(!message.contains("protocol: missing a location header"));
    }
}
