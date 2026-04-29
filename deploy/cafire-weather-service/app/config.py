from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


CALIFORNIA_BOUNDS = (-125.2, -113.5, 31.0, 43.0)


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _split_int_csv(value: str) -> list[int]:
    return [int(item) for item in _split_csv(value)]


def _split_lower_csv(value: str) -> list[str]:
    return [item.lower() for item in _split_csv(value)]


def parse_hour_spec(value: str) -> list[int]:
    hours: list[int] = []
    for part in _split_csv(value):
        if "-" in part:
            raw_start, raw_end = part.split("-", 1)
            start = int(raw_start)
            end = int(raw_end)
            if end < start:
                raise ValueError(f"invalid hour range: {part}")
            hours.extend(range(start, end + 1))
        else:
            hours.append(int(part))
    return sorted(set(hours))


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    artifact_root: Path
    rustwx_cache_dir: Path
    glm_dir: Path
    api_key: str | None
    public_site_enabled: bool
    allow_outside_california: bool
    default_model: str
    default_source: str
    default_domain: str
    default_products: list[str]
    default_forecast_hours: list[int]
    default_width: int
    default_height: int
    meteogram_warm_enabled: bool
    meteogram_warm_in_api: bool
    meteogram_warm_interval_sec: int
    meteogram_warm_chunk_size: int
    meteogram_warm_hours: list[int]
    meteogram_warm_lat: float
    meteogram_warm_lon: float
    meteogram_warm_status_path: Path
    meteogram_warm_variables: list[str]
    meteogram_sample_workers: int
    meteogram_sample_chunk_size: int
    full_run_min_age_hours: int
    fast_meteogram_store_enabled: bool
    fast_meteogram_store_interval_sec: int
    fast_meteogram_store_bounds: tuple[float, float, float, float]
    pressure_volume_enabled: bool
    pressure_volume_base_url: str | None
    pressure_volume_timeout_sec: float
    pressure_volume_store_path: Path
    pressure_volume_partial_store_path: Path
    pressure_volume_renderer_path: Path
    pressure_cross_section_style_version: str
    pressure_cross_section_render_max_active: int
    pressure_cross_section_loop_max_active: int
    pressure_cross_section_render_timeout_sec: float
    pressure_cross_section_loop_timeout_sec: float
    pressure_cross_section_default_top_hpa: float
    pressure_cross_section_default_width: int
    pressure_cross_section_default_height: int
    pressure_cross_section_min_spacing_km: float
    pressure_cross_section_max_spacing_km: float
    pressure_volume_builder_enabled: bool
    pressure_volume_builder_interval_sec: int
    pressure_volume_builder_path: Path
    pressure_volume_builder_store_root: Path
    pressure_volume_builder_status_path: Path
    pressure_volume_builder_load_parallelism: int
    pressure_volume_builder_keep_completed: int
    pressure_volume_builder_require_static_manifest: bool
    pressure_volume_partial_enabled: bool
    pressure_volume_partial_require_static_manifest: bool
    pressure_volume_partial_max_hour: int
    pressure_volume_builder_start_hour: int
    pressure_volume_builder_end_hour: int
    pressure_volume_builder_bounds: tuple[float, float, float, float]
    lightning_enabled: bool
    lightning_interval_sec: int
    lightning_domain: str
    lightning_label: str
    lightning_satellite: str
    lightning_fetch_count: int
    lightning_lookback_hours: int
    lightning_max_age_min: float
    lightning_width: int
    lightning_height: int
    satellite_enabled: bool
    satellite_interval_sec: int
    satellite_domain: str
    satellite_label: str
    satellite_satellite: str
    satellite_abi_product: str
    satellite_products: list[str]
    satellite_scan_lookback_hours: int
    satellite_discovery_retries: int
    satellite_retry_sleep_ms: int
    satellite_download_glm: bool
    satellite_glm_fetch_count: int
    satellite_glm_lookback_hours: int
    satellite_glm_max_age_min: float
    satellite_width: int
    satellite_height: int
    satellite_still_widths: list[int]
    satellite_still_formats: list[str]
    satellite_still_allow_upscale: bool
    satellite_loop_enabled: bool
    satellite_loop_max_frames: int
    satellite_loop_frame_ms: int
    satellite_loop_widths: list[int]
    satellite_loop_durations_min: list[int]
    satellite_loop_formats: list[str]
    satellite_loop_gif_max_duration_min: int
    cache_cleanup_enabled: bool
    cache_cleanup_interval_sec: int
    cache_cleanup_max_age_hours: int
    cache_cleanup_max_cache_gb: float
    cache_cleanup_target_cache_gb: float
    cache_cleanup_min_free_gb: float
    cache_cleanup_target_free_gb: float
    cache_cleanup_emergency_min_age_hours: int
    static_map_worker_enabled: bool
    static_map_worker_interval_sec: int
    static_map_worker_parallelism: int
    static_map_worker_cycle_lookback_hours: int
    static_map_backfill_batch_hours: int
    static_map_smoke_interval_hours: int
    static_map_brand_text: str | None
    static_map_webp_enabled: bool
    static_map_webp_quality: int
    r2_account_id: str | None
    r2_bucket: str | None
    r2_access_key_id: str | None
    r2_secret_access_key: str | None
    r2_endpoint_url: str | None
    public_artifact_base_url: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        artifact_root = Path(os.environ.get("ARTIFACT_ROOT", "/data/artifacts"))
        cache_dir = Path(os.environ.get("RUSTWX_CACHE_DIR", "/data/cache"))
        glm_dir = Path(os.environ.get("RUSTWX_GLM_DIR", "/data/glm"))
        pressure_volume_store_path = Path(
            os.environ.get("PRESSURE_VOLUME_STORE_PATH", "/data/volume-stores/current")
        )
        pressure_volume_partial_store_path = Path(
            os.environ.get("PRESSURE_VOLUME_PARTIAL_STORE_PATH", "/data/volume-stores/latest-partial")
        )
        pressure_volume_builder_store_root = Path(
            os.environ.get("PRESSURE_VOLUME_BUILDER_STORE_ROOT", str(pressure_volume_store_path.parent))
        )
        default_products = _split_csv(
            os.environ.get(
                "DEFAULT_PRODUCTS",
                "2m_temperature_10m_winds,2m_relative_humidity_10m_winds,"
                "2m_dewpoint_10m_winds,10m_wind_gusts,vpd_2m,"
                "fire_weather_composite,qpf_1h,10m_wind_1h_max,"
                "2m_temp_0_24h_range,2m_temp_24_48h_range,2m_temp_0_48h_range,"
                "visibility,smoke_pm25_native,smoke_column",
            )
        )
        return cls(
            artifact_root=artifact_root,
            rustwx_cache_dir=cache_dir,
            glm_dir=glm_dir,
            api_key=os.environ.get("SERVICE_API_KEY") or None,
            public_site_enabled=_bool_env("PUBLIC_SITE_ENABLED", True),
            allow_outside_california=_bool_env("ALLOW_OUTSIDE_CALIFORNIA", False),
            default_model=os.environ.get("DEFAULT_MODEL", "hrrr"),
            default_source=os.environ.get("DEFAULT_SOURCE", "nomads"),
            default_domain=os.environ.get("DEFAULT_DOMAIN", "california"),
            default_products=default_products,
            default_forecast_hours=parse_hour_spec(os.environ.get("DEFAULT_FORECAST_HOURS", "0-48")),
            default_width=int(os.environ.get("DEFAULT_WIDTH", "1400")),
            default_height=int(os.environ.get("DEFAULT_HEIGHT", "1000")),
            meteogram_warm_enabled=_bool_env("METEOGRAM_WARM_ENABLED", True),
            meteogram_warm_in_api=_bool_env("METEOGRAM_WARM_IN_API", False),
            meteogram_warm_interval_sec=int(os.environ.get("METEOGRAM_WARM_INTERVAL_SEC", "900")),
            meteogram_warm_chunk_size=max(1, int(os.environ.get("METEOGRAM_WARM_CHUNK_SIZE", "6"))),
            meteogram_warm_hours=parse_hour_spec(os.environ.get("METEOGRAM_WARM_HOURS", "0-48")),
            meteogram_warm_lat=float(os.environ.get("METEOGRAM_WARM_LAT", "37.25")),
            meteogram_warm_lon=float(os.environ.get("METEOGRAM_WARM_LON", "-119.5")),
            meteogram_warm_status_path=Path(
                os.environ.get("METEOGRAM_WARM_STATUS_PATH", "/data/artifacts/meteogram_warm_status.json")
            ),
            meteogram_warm_variables=_split_csv(os.environ.get("METEOGRAM_WARM_VARIABLES", "")),
            meteogram_sample_workers=max(1, int(os.environ.get("METEOGRAM_SAMPLE_WORKERS", "4"))),
            meteogram_sample_chunk_size=max(1, int(os.environ.get("METEOGRAM_SAMPLE_CHUNK_SIZE", "7"))),
            full_run_min_age_hours=max(2, int(os.environ.get("FULL_RUN_MIN_AGE_HOURS", "6"))),
            fast_meteogram_store_enabled=_bool_env("FAST_METEOGRAM_STORE_ENABLED", True),
            fast_meteogram_store_interval_sec=int(os.environ.get("FAST_METEOGRAM_STORE_INTERVAL_SEC", "900")),
            fast_meteogram_store_bounds=tuple(
                float(part)
                for part in os.environ.get("FAST_METEOGRAM_STORE_BOUNDS", "-125.2,-113.5,31.0,43.0").split(",")
            ),
            pressure_volume_enabled=_bool_env("PRESSURE_VOLUME_ENABLED", False),
            pressure_volume_base_url=os.environ.get("PRESSURE_VOLUME_BASE_URL") or None,
            pressure_volume_timeout_sec=max(0.2, float(os.environ.get("PRESSURE_VOLUME_TIMEOUT_SEC", "10"))),
            pressure_volume_store_path=pressure_volume_store_path,
            pressure_volume_partial_store_path=pressure_volume_partial_store_path,
            pressure_volume_renderer_path=Path(
                os.environ.get("PRESSURE_VOLUME_RENDERER_PATH", "/app/bin/volume_store_cross_section_render")
            ),
            pressure_cross_section_style_version=os.environ.get(
                "PRESSURE_CROSS_SECTION_STYLE_VERSION", "cross_section_plot_v2"
            ),
            pressure_cross_section_render_max_active=max(
                1, int(os.environ.get("PRESSURE_CROSS_SECTION_RENDER_MAX_ACTIVE", "3"))
            ),
            pressure_cross_section_loop_max_active=max(
                1, int(os.environ.get("PRESSURE_CROSS_SECTION_LOOP_MAX_ACTIVE", "2"))
            ),
            pressure_cross_section_render_timeout_sec=max(
                5.0, float(os.environ.get("PRESSURE_CROSS_SECTION_RENDER_TIMEOUT_SEC", "180"))
            ),
            pressure_cross_section_loop_timeout_sec=max(
                30.0, float(os.environ.get("PRESSURE_CROSS_SECTION_LOOP_TIMEOUT_SEC", "900"))
            ),
            pressure_cross_section_default_top_hpa=max(
                10.0, float(os.environ.get("PRESSURE_CROSS_SECTION_DEFAULT_TOP_HPA", "100"))
            ),
            pressure_cross_section_default_width=max(
                600, int(os.environ.get("PRESSURE_CROSS_SECTION_DEFAULT_WIDTH", "1400"))
            ),
            pressure_cross_section_default_height=max(
                420, int(os.environ.get("PRESSURE_CROSS_SECTION_DEFAULT_HEIGHT", "820"))
            ),
            pressure_cross_section_min_spacing_km=max(
                0.5, float(os.environ.get("PRESSURE_CROSS_SECTION_MIN_SPACING_KM", "1"))
            ),
            pressure_cross_section_max_spacing_km=max(
                1.0, float(os.environ.get("PRESSURE_CROSS_SECTION_MAX_SPACING_KM", "80"))
            ),
            pressure_volume_builder_enabled=_bool_env("PRESSURE_VOLUME_BUILDER_ENABLED", True),
            pressure_volume_builder_interval_sec=max(
                60, int(os.environ.get("PRESSURE_VOLUME_BUILDER_INTERVAL_SEC", "900"))
            ),
            pressure_volume_builder_path=Path(
                os.environ.get("PRESSURE_VOLUME_BUILDER_PATH", "/app/bin/hrrr_pressure_volume_store")
            ),
            pressure_volume_builder_store_root=pressure_volume_builder_store_root,
            pressure_volume_builder_status_path=Path(
                os.environ.get(
                    "PRESSURE_VOLUME_BUILDER_STATUS_PATH",
                    str(pressure_volume_builder_store_root / "builder_status.json"),
                )
            ),
            pressure_volume_builder_load_parallelism=max(
                1, int(os.environ.get("PRESSURE_VOLUME_BUILDER_LOAD_PARALLELISM", "4"))
            ),
            pressure_volume_builder_keep_completed=max(
                1, int(os.environ.get("PRESSURE_VOLUME_BUILDER_KEEP_COMPLETED", "2"))
            ),
            pressure_volume_builder_require_static_manifest=_bool_env(
                "PRESSURE_VOLUME_BUILDER_REQUIRE_STATIC_MANIFEST", True
            ),
            pressure_volume_partial_enabled=_bool_env("PRESSURE_VOLUME_PARTIAL_ENABLED", True),
            pressure_volume_partial_require_static_manifest=_bool_env(
                "PRESSURE_VOLUME_PARTIAL_REQUIRE_STATIC_MANIFEST", True
            ),
            pressure_volume_partial_max_hour=max(
                0, int(os.environ.get("PRESSURE_VOLUME_PARTIAL_MAX_HOUR", "18"))
            ),
            pressure_volume_builder_start_hour=max(
                0, int(os.environ.get("PRESSURE_VOLUME_BUILDER_START_HOUR", "0"))
            ),
            pressure_volume_builder_end_hour=max(
                0, int(os.environ.get("PRESSURE_VOLUME_BUILDER_END_HOUR", "48"))
            ),
            pressure_volume_builder_bounds=tuple(
                float(part)
                for part in os.environ.get("PRESSURE_VOLUME_BUILDER_BOUNDS", "-125.2,-113.5,31.0,43.0").split(",")
            ),
            lightning_enabled=_bool_env("LIGHTNING_ENABLED", True),
            lightning_interval_sec=int(os.environ.get("LIGHTNING_INTERVAL_SEC", "30")),
            lightning_domain=os.environ.get("LIGHTNING_DOMAIN", "california"),
            lightning_label=os.environ.get("LIGHTNING_LABEL", "California GLM Lightning"),
            lightning_satellite=os.environ.get("LIGHTNING_SATELLITE", "goes18"),
            lightning_fetch_count=max(1, int(os.environ.get("LIGHTNING_FETCH_COUNT", "90"))),
            lightning_lookback_hours=max(1, int(os.environ.get("LIGHTNING_LOOKBACK_HOURS", "3"))),
            lightning_max_age_min=max(1.0, float(os.environ.get("LIGHTNING_MAX_AGE_MIN", "30"))),
            lightning_width=max(400, int(os.environ.get("LIGHTNING_WIDTH", "1400"))),
            lightning_height=max(400, int(os.environ.get("LIGHTNING_HEIGHT", "1100"))),
            satellite_enabled=_bool_env("SATELLITE_ENABLED", True),
            satellite_interval_sec=max(60, int(os.environ.get("SATELLITE_INTERVAL_SEC", "300"))),
            satellite_domain=os.environ.get("SATELLITE_DOMAIN", "pacific_southwest"),
            satellite_label=os.environ.get("SATELLITE_LABEL", "Pacific Southwest Satellite"),
            satellite_satellite=os.environ.get("SATELLITE_SATELLITE", "goes18"),
            satellite_abi_product=os.environ.get("SATELLITE_ABI_PRODUCT", "ABI-L2-CMIPC"),
            satellite_products=_split_csv(
                os.environ.get(
                    "SATELLITE_PRODUCTS",
                    "goes_geocolor,goes_glm_fed_geocolor,goes_airmass_rgb,"
                    "goes_sandwich_rgb,goes_day_night_cloud_micro_combo_rgb,"
                    "goes_fire_temperature_rgb,goes_dust_rgb,"
                    "goes_abi_band_01,goes_abi_band_02,goes_abi_band_03,goes_abi_band_04,"
                    "goes_abi_band_05,goes_abi_band_06,goes_abi_band_07,goes_abi_band_08,"
                    "goes_abi_band_09,goes_abi_band_10,goes_abi_band_11,goes_abi_band_12,"
                    "goes_abi_band_13,goes_abi_band_14,goes_abi_band_15,goes_abi_band_16",
                )
            ),
            satellite_scan_lookback_hours=max(1, int(os.environ.get("SATELLITE_SCAN_LOOKBACK_HOURS", "6"))),
            satellite_discovery_retries=max(0, int(os.environ.get("SATELLITE_DISCOVERY_RETRIES", "2"))),
            satellite_retry_sleep_ms=max(0, int(os.environ.get("SATELLITE_RETRY_SLEEP_MS", "20000"))),
            satellite_download_glm=_bool_env("SATELLITE_DOWNLOAD_GLM", True),
            satellite_glm_fetch_count=max(1, int(os.environ.get("SATELLITE_GLM_FETCH_COUNT", "90"))),
            satellite_glm_lookback_hours=max(1, int(os.environ.get("SATELLITE_GLM_LOOKBACK_HOURS", "3"))),
            satellite_glm_max_age_min=max(1.0, float(os.environ.get("SATELLITE_GLM_MAX_AGE_MIN", "30"))),
            satellite_width=max(400, int(os.environ.get("SATELLITE_WIDTH", "1400"))),
            satellite_height=max(400, int(os.environ.get("SATELLITE_HEIGHT", "1100"))),
            satellite_still_widths=sorted(
                set(
                    width
                    for width in _split_int_csv(
                        os.environ.get("SATELLITE_STILL_WIDTHS", "600")
                    )
                    if width >= 120
                )
            ),
            satellite_still_formats=[
                fmt
                for fmt in _split_lower_csv(os.environ.get("SATELLITE_STILL_FORMATS", "webp"))
                if fmt in {"webp", "png"}
            ]
            or ["webp"],
            satellite_still_allow_upscale=_bool_env("SATELLITE_STILL_ALLOW_UPSCALE", False),
            satellite_loop_enabled=_bool_env("SATELLITE_LOOP_ENABLED", True),
            satellite_loop_max_frames=max(1, int(os.environ.get("SATELLITE_LOOP_MAX_FRAMES", "6"))),
            satellite_loop_frame_ms=max(80, int(os.environ.get("SATELLITE_LOOP_FRAME_MS", "450"))),
            satellite_loop_widths=sorted(
                set(
                    width
                    for width in _split_int_csv(
                        os.environ.get(
                            "SATELLITE_LOOP_WIDTHS",
                            os.environ.get("SATELLITE_LOOP_WIDTH", "600"),
                        )
                    )
                    if width >= 120
                )
            )
            or [600],
            satellite_loop_durations_min=sorted(
                set(
                    duration
                    for duration in _split_int_csv(os.environ.get("SATELLITE_LOOP_DURATIONS_MIN", "30"))
                    if duration > 0
                )
            )
            or [30],
            satellite_loop_formats=[
                fmt
                for fmt in _split_lower_csv(os.environ.get("SATELLITE_LOOP_FORMATS", "webp"))
                if fmt in {"webp", "gif"}
            ]
            or ["webp"],
            satellite_loop_gif_max_duration_min=max(
                0, int(os.environ.get("SATELLITE_LOOP_GIF_MAX_DURATION_MIN", "0"))
            ),
            cache_cleanup_enabled=_bool_env("CACHE_CLEANUP_ENABLED", True),
            cache_cleanup_interval_sec=int(os.environ.get("CACHE_CLEANUP_INTERVAL_SEC", "1800")),
            cache_cleanup_max_age_hours=max(1, int(os.environ.get("CACHE_CLEANUP_MAX_AGE_HOURS", "30"))),
            cache_cleanup_max_cache_gb=max(1.0, float(os.environ.get("CACHE_CLEANUP_MAX_CACHE_GB", "300"))),
            cache_cleanup_target_cache_gb=max(
                1.0,
                float(
                    os.environ.get(
                        "CACHE_CLEANUP_TARGET_CACHE_GB",
                        str(max(1.0, float(os.environ.get("CACHE_CLEANUP_MAX_CACHE_GB", "300")) * 0.95)),
                    )
                ),
            ),
            cache_cleanup_min_free_gb=max(1.0, float(os.environ.get("CACHE_CLEANUP_MIN_FREE_GB", "160"))),
            cache_cleanup_target_free_gb=max(1.0, float(os.environ.get("CACHE_CLEANUP_TARGET_FREE_GB", "220"))),
            cache_cleanup_emergency_min_age_hours=max(
                0, int(os.environ.get("CACHE_CLEANUP_EMERGENCY_MIN_AGE_HOURS", "4"))
            ),
            static_map_worker_enabled=_bool_env("STATIC_MAP_WORKER_ENABLED", True),
            static_map_worker_interval_sec=int(os.environ.get("STATIC_MAP_WORKER_INTERVAL_SEC", "30")),
            static_map_worker_parallelism=max(1, int(os.environ.get("STATIC_MAP_WORKER_PARALLELISM", "3"))),
            static_map_worker_cycle_lookback_hours=max(
                1, int(os.environ.get("STATIC_MAP_WORKER_CYCLE_LOOKBACK_HOURS", "8"))
            ),
            static_map_backfill_batch_hours=max(1, int(os.environ.get("STATIC_MAP_BACKFILL_BATCH_HOURS", "3"))),
            static_map_smoke_interval_hours=max(1, int(os.environ.get("STATIC_MAP_SMOKE_INTERVAL_HOURS", "1"))),
            static_map_brand_text=os.environ.get("STATIC_MAP_BRAND_TEXT", "California Wildfire Tracking") or None,
            static_map_webp_enabled=_bool_env("STATIC_MAP_WEBP_ENABLED", True),
            static_map_webp_quality=max(1, min(100, int(os.environ.get("STATIC_MAP_WEBP_QUALITY", "72")))),
            r2_account_id=os.environ.get("R2_ACCOUNT_ID") or None,
            r2_bucket=os.environ.get("R2_BUCKET") or None,
            r2_access_key_id=os.environ.get("R2_ACCESS_KEY_ID") or None,
            r2_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY") or None,
            r2_endpoint_url=os.environ.get("R2_ENDPOINT_URL") or None,
            public_artifact_base_url=os.environ.get("PUBLIC_ARTIFACT_BASE_URL") or None,
        )

    def ensure_dirs(self) -> None:
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.rustwx_cache_dir.mkdir(parents=True, exist_ok=True)
        self.glm_dir.mkdir(parents=True, exist_ok=True)
        self.meteogram_warm_status_path.parent.mkdir(parents=True, exist_ok=True)
        self.pressure_volume_builder_store_root.mkdir(parents=True, exist_ok=True)
        self.pressure_volume_builder_status_path.parent.mkdir(parents=True, exist_ok=True)

    def r2_enabled(self) -> bool:
        return all(
            [
                self.r2_bucket,
                self.r2_access_key_id,
                self.r2_secret_access_key,
                self.r2_endpoint_url,
            ]
        )


def point_in_california_buffer(lat: float, lon: float) -> bool:
    west, east, south, north = CALIFORNIA_BOUNDS
    return west <= lon <= east and south <= lat <= north


settings = Settings.from_env()
