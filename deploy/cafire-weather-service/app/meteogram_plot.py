from __future__ import annotations

import hashlib
import json
import math
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import Settings
from .storage import ArtifactStore


PLOT_VARIABLES = [
    "temperature_2m_c",
    "dewpoint_2m_c",
    "wetbulb_2m_c",
    "relative_humidity_2m_pct",
    "wind_speed_10m_ms",
    "wind_direction_10m_deg",
    "wind_gust_10m_ms",
    "precip_hourly_mm",
    "precip_accum_mm",
    "cloud_low_pct",
    "cloud_middle_pct",
    "cloud_high_pct",
    "mslp_hpa",
    "vpd_2m_hpa",
    "hdw",
    "fire_weather_composite",
]

_MS_TO_MPH = 2.2369362921
_MM_TO_IN = 1.0 / 25.4


def _safe(value: Any) -> float:
    if value is None:
        return math.nan
    try:
        out = float(value)
    except (TypeError, ValueError):
        return math.nan
    return out if math.isfinite(out) else math.nan


def _c_to_f(value: Any) -> float:
    value = _safe(value)
    return value * 9.0 / 5.0 + 32.0 if math.isfinite(value) else math.nan


def _ms_to_mph(value: Any) -> float:
    value = _safe(value)
    return value * _MS_TO_MPH if math.isfinite(value) else math.nan


def _nanmax(values: Any, default: float = 1.0) -> float:
    import numpy as np

    array = np.asarray(values, dtype=float)
    finite = array[np.isfinite(array)]
    if finite.size == 0:
        return default
    return float(np.max(finite))


def _nanmin(values: Any, default: float = 0.0) -> float:
    import numpy as np

    array = np.asarray(values, dtype=float)
    finite = array[np.isfinite(array)]
    if finite.size == 0:
        return default
    return float(np.min(finite))


def _slug(value: str, fallback: str = "point") -> str:
    out = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return out or fallback


def _coord_slug(value: float) -> str:
    prefix = "p" if value >= 0 else "m"
    return prefix + f"{abs(value):.4f}".replace(".", "p")


def _artifact_paths(
    *,
    settings: Settings,
    date: str,
    cycle: int,
    lat: float,
    lon: float,
    forecast_hours: list[int],
    label: str,
) -> dict[str, Any]:
    hour_label = f"f{min(forecast_hours):03d}-f{max(forecast_hours):03d}"
    digest_source = json.dumps(
        {
            "date": date,
            "cycle": cycle,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "hours": forecast_hours,
            "v": 2,
        },
        sort_keys=True,
    )
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:10]
    name = f"{_slug(label)}_{_coord_slug(lat)}_{_coord_slug(lon)}_{digest}"
    key_prefix = f"meteograms/hrrr/{date}/{cycle:02d}z/{hour_label}"
    png_key = f"{key_prefix}/{name}.png"
    json_key = f"{key_prefix}/{name}.json"
    return {
        "png_key": png_key,
        "json_key": json_key,
        "local_png": settings.artifact_root / png_key,
        "local_json": settings.artifact_root / json_key,
    }


def _public_url(settings: Settings, key: str) -> str:
    if settings.public_artifact_base_url:
        return f"{settings.public_artifact_base_url.rstrip('/')}/{key}"
    return f"/artifacts/{key}"


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _series(report: dict[str, Any]) -> dict[str, Any]:
    hours = report.get("hours") or []
    times: list[datetime] = []
    fhours: list[int] = []
    t_f: list[float] = []
    td_f: list[float] = []
    tw_f: list[float] = []
    rh: list[float] = []
    wind: list[float] = []
    gust: list[float] = []
    direction: list[float] = []
    u: list[float] = []
    v: list[float] = []
    precip_1h: list[float] = []
    precip_total: list[float] = []
    low_cloud: list[float] = []
    mid_cloud: list[float] = []
    high_cloud: list[float] = []
    mslp: list[float] = []
    vpd: list[float] = []
    hdw: list[float] = []
    fire_comp: list[float] = []

    for hour in hours:
        values = hour.get("values") or {}
        times.append(_parse_time(hour["valid_time_utc"]))
        fhours.append(int(hour.get("forecast_hour", 0)))
        t_f.append(_c_to_f(values.get("temperature_2m_c")))
        td_f.append(_c_to_f(values.get("dewpoint_2m_c")))
        tw_f.append(_c_to_f(values.get("wetbulb_2m_c")))
        rh.append(_safe(values.get("relative_humidity_2m_pct")))
        wind_mph = _ms_to_mph(values.get("wind_speed_10m_ms"))
        wind_dir = _safe(values.get("wind_direction_10m_deg"))
        wind.append(wind_mph)
        gust.append(_ms_to_mph(values.get("wind_gust_10m_ms")))
        direction.append(wind_dir)
        if math.isfinite(wind_mph) and math.isfinite(wind_dir):
            radians = math.radians(wind_dir)
            u.append(-wind_mph * math.sin(radians))
            v.append(-wind_mph * math.cos(radians))
        else:
            u.append(math.nan)
            v.append(math.nan)
        precip_1h.append(_safe(values.get("precip_hourly_mm")) * _MM_TO_IN)
        precip_total.append(_safe(values.get("precip_accum_mm")) * _MM_TO_IN)
        low_cloud.append(_safe(values.get("cloud_low_pct")))
        mid_cloud.append(_safe(values.get("cloud_middle_pct")))
        high_cloud.append(_safe(values.get("cloud_high_pct")))
        mslp.append(_safe(values.get("mslp_hpa")))
        vpd.append(_safe(values.get("vpd_2m_hpa")) / 10.0)
        hdw.append(_safe(values.get("hdw")))
        fire_comp.append(_safe(values.get("fire_weather_composite")))

    return {
        "times": times,
        "fhours": fhours,
        "temperature_f": t_f,
        "dewpoint_f": td_f,
        "wetbulb_f": tw_f,
        "rh_pct": rh,
        "wind_mph": wind,
        "gust_mph": gust,
        "wind_dir_deg": direction,
        "u_mph": u,
        "v_mph": v,
        "precip_1h_in": precip_1h,
        "precip_total_in": precip_total,
        "cloud_low_pct": low_cloud,
        "cloud_middle_pct": mid_cloud,
        "cloud_high_pct": high_cloud,
        "mslp_hpa": mslp,
        "vpd_kpa": vpd,
        "hdw": hdw,
        "fire_weather_composite": fire_comp,
    }


def _shade_nights(ax: Any, times: list[datetime]) -> None:
    if not times:
        return
    local_zone = ZoneInfo("America/Los_Angeles")
    start_local = times[0].astimezone(local_zone).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = times[-1].astimezone(local_zone).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2)
    current = start_local - timedelta(days=1)
    while current <= end_local:
        night_start = current.replace(hour=20).astimezone(UTC)
        night_end = (current + timedelta(days=1)).replace(hour=6).astimezone(UTC)
        left = max(night_start, times[0])
        right = min(night_end, times[-1])
        if left < right:
            ax.axvspan(left, right, color="#202a44", alpha=0.10, lw=0, zorder=0)
        current += timedelta(days=1)


def _format_local_tick(x: float, _pos: Any = None) -> str:
    import matplotlib.dates as mdates

    local = mdates.num2date(x, tz=UTC).astimezone(ZoneInfo("America/Los_Angeles"))
    return f"{local:%a}\n{local:%H} PT"


def _plot(report: dict[str, Any], label: str, out_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import numpy as np

    data = _series(report)
    times = data["times"]
    if not times:
        raise RuntimeError("rustwx returned no meteogram hours")

    run = report.get("run") or {}
    date = str(run.get("date_yyyymmdd", "unknown"))
    cycle = int(run.get("cycle_utc", 0))
    point = report.get("point") or {}
    grid = (report.get("hours") or [{}])[0].get("grid_point") or {}
    lat = _safe(point.get("lat_deg"))
    lon = _safe(point.get("lon_deg"))
    grid_lat = _safe(grid.get("lat_deg"))
    grid_lon = _safe(grid.get("lon_deg"))

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.labelsize": 10.5,
            "xtick.labelsize": 9.5,
            "ytick.labelsize": 9.5,
            "legend.fontsize": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.alpha": 0.28,
        }
    )

    fig, axes = plt.subplots(
        6,
        1,
        figsize=(14.0, 10.8),
        dpi=145,
        sharex=True,
        gridspec_kw={"height_ratios": [1.45, 1.0, 1.35, 1.0, 1.0, 1.25], "hspace": 0.24},
    )
    fig.patch.set_facecolor("#f7f9fb")
    ax_t, ax_rh, ax_wind, ax_precip, ax_cloud, ax_fire = axes
    for ax in axes:
        ax.set_facecolor("#ffffff")
        _shade_nights(ax, times)
        ax.grid(True, axis="y", color="#bcc7cf", lw=0.55)
        ax.grid(True, axis="x", color="#d8dee4", lw=0.45)
        ax.tick_params(colors="#263238")
        ax.yaxis.label.set_color("#263238")

    temp = np.asarray(data["temperature_f"], dtype=float)
    dewpoint = np.asarray(data["dewpoint_f"], dtype=float)
    wetbulb = np.asarray(data["wetbulb_f"], dtype=float)
    ymin = math.floor((_nanmin(np.r_[temp, dewpoint, wetbulb], 40.0) - 5.0) / 5.0) * 5.0
    ymax = math.ceil((_nanmax(np.r_[temp, dewpoint, wetbulb], 80.0) + 5.0) / 5.0) * 5.0
    ax_t.plot(times, temp, color="#d84b2a", lw=2.3, label="Temp")
    ax_t.plot(times, dewpoint, color="#168a45", lw=2.0, label="Dewpoint")
    ax_t.plot(times, wetbulb, color="#2474b7", lw=1.8, ls="--", label="Wet bulb")
    ax_t.fill_between(times, dewpoint, temp, where=temp >= dewpoint, color="#d84b2a", alpha=0.07)
    ax_t.set_ylabel("Deg F")
    ax_t.set_ylim(ymin, ymax)
    ax_t.set_title("Temperature, Dewpoint, Wet Bulb")
    ax_t.legend(loc="upper right", ncol=3, frameon=False)

    rh = np.asarray(data["rh_pct"], dtype=float)
    ax_rh.fill_between(times, 0, rh, color="#298c4a", alpha=0.18)
    ax_rh.plot(times, rh, color="#1f7a3f", lw=2.0, label="RH")
    ax_rh.axhline(20, color="#c92727", lw=1.0, ls=":", label="20%")
    ax_rh.axhline(30, color="#e37b22", lw=0.9, ls=":", label="30%")
    ax_rh.fill_between(
        times,
        0,
        np.where(rh < 20, rh, np.nan),
        color="#d52222",
        alpha=0.30,
        hatch="////",
        edgecolor="#9c1919",
        linewidth=0.0,
    )
    ax_rh.set_ylim(0, 100)
    ax_rh.set_ylabel("RH (%)")
    ax_rh.set_title("Relative Humidity")
    ax_rh.legend(loc="upper right", ncol=3, frameon=False)

    wind = np.asarray(data["wind_mph"], dtype=float)
    gust = np.asarray(data["gust_mph"], dtype=float)
    ax_wind.plot(times, wind, color="#71429a", lw=2.0, label="10 m wind")
    ax_wind.plot(times, gust, color="#e07522", lw=1.8, ls="--", label="Gust")
    ax_wind.fill_between(times, wind, gust, where=gust >= wind, color="#e07522", alpha=0.10)
    ax_wind.axhline(25, color="#c92727", lw=1.0, ls=":", label="25 mph")
    ax_wind.fill_between(
        times,
        0,
        np.where(wind > 25, wind, np.nan),
        color="#d52222",
        alpha=0.18,
        hatch="\\\\",
        edgecolor="#9c1919",
        linewidth=0.0,
    )
    wind_top = max(15.0, _nanmax(np.r_[wind, gust], 20.0) * 1.28)
    ax_wind.set_ylim(0, wind_top)
    ax_wind.set_ylabel("mph")
    ax_wind.set_title("10 m Wind and Gust")
    ax_wind.legend(loc="upper right", ncol=3, frameon=False)
    u = np.asarray(data["u_mph"], dtype=float)
    v = np.asarray(data["v_mph"], dtype=float)
    finite_barbs = np.isfinite(u) & np.isfinite(v)
    if np.any(finite_barbs):
        step = max(1, len(times) // 28)
        selected = np.arange(len(times))[::step]
        selected = selected[finite_barbs[selected]]
        if selected.size:
            barb_x = mdates.date2num([times[int(idx)] for idx in selected])
            barb_y = np.full_like(barb_x, 1.4)
            ax_wind.barbs(
                barb_x,
                barb_y,
                u[selected],
                v[selected],
                length=5.2,
                linewidth=0.85,
                color="#2e3940",
                sizes={"emptybarb": 0.16},
            )

    precip_1h = np.asarray(data["precip_1h_in"], dtype=float)
    precip_total = np.asarray(data["precip_total_in"], dtype=float)
    bar_width = (mdates.date2num(times[1]) - mdates.date2num(times[0])) * 0.82 if len(times) > 1 else 0.035
    ax_precip.bar(times, precip_1h, width=bar_width, color="#2f80c1", alpha=0.78, label="1 h")
    ax_precip.set_ylim(0, max(0.04, _nanmax(precip_1h, 0.02) * 1.35))
    ax_precip.set_ylabel("1 h (in)")
    ax_precip.set_title("Precipitation")
    ax_precip_t = ax_precip.twinx()
    ax_precip_t.plot(times, precip_total, color="#0a3768", lw=1.8, label="Run total")
    ax_precip_t.set_ylim(0, max(0.05, _nanmax(precip_total, 0.02) * 1.25))
    ax_precip_t.set_ylabel("Total (in)")
    ax_precip_t.spines["top"].set_visible(False)
    ax_precip.legend(loc="upper left", frameon=False)
    ax_precip_t.legend(loc="upper right", frameon=False)

    low = np.nan_to_num(np.asarray(data["cloud_low_pct"], dtype=float), nan=0.0)
    mid = np.nan_to_num(np.asarray(data["cloud_middle_pct"], dtype=float), nan=0.0)
    high = np.nan_to_num(np.asarray(data["cloud_high_pct"], dtype=float), nan=0.0)
    ax_cloud.stackplot(
        times,
        low,
        mid,
        high,
        labels=["Low", "Mid", "High"],
        colors=["#495967", "#8b99a6", "#d5dce3"],
        alpha=0.88,
    )
    ax_cloud.set_ylim(0, 300)
    ax_cloud.set_ylabel("Cloud %")
    ax_cloud.set_title("Cloud Cover and Mean Sea-Level Pressure")
    ax_cloud.legend(loc="upper left", ncol=3, frameon=False)
    pressure = np.asarray(data["mslp_hpa"], dtype=float)
    ax_cloud_t = ax_cloud.twinx()
    if np.any(np.isfinite(pressure)):
        ax_cloud_t.plot(times, pressure, color="#15191c", lw=1.6, label="MSLP")
        pad = max(1.0, (_nanmax(pressure) - _nanmin(pressure)) * 0.20)
        ax_cloud_t.set_ylim(_nanmin(pressure) - pad, _nanmax(pressure) + pad)
    ax_cloud_t.set_ylabel("hPa")
    ax_cloud_t.spines["top"].set_visible(False)

    vpd = np.asarray(data["vpd_kpa"], dtype=float)
    hdw = np.asarray(data["hdw"], dtype=float)
    fire_comp = np.asarray(data["fire_weather_composite"], dtype=float)
    ax_fire.plot(times, vpd, color="#b82f24", lw=2.2, label="VPD")
    ax_fire.fill_between(times, 0, vpd, color="#b82f24", alpha=0.14)
    ax_fire.axhline(2.0, color="#7a1111", lw=1.0, ls=":", label="2.0 kPa")
    ax_fire.fill_between(
        times,
        0,
        np.where(vpd > 2.0, vpd, np.nan),
        color="#7a1111",
        alpha=0.32,
        hatch="xx",
        edgecolor="#4d0909",
        linewidth=0.0,
    )
    ax_fire.set_ylim(0, max(2.5, _nanmax(vpd, 1.0) * 1.18))
    ax_fire.set_ylabel("VPD (kPa)")
    ax_fire.set_title("Fire Weather: VPD, HDW, Composite")
    ax_fire_t = ax_fire.twinx()
    if np.any(np.isfinite(hdw)):
        ax_fire_t.plot(times, hdw, color="#283b9b", lw=1.6, ls="--", label="HDW")
    if np.any(np.isfinite(fire_comp)):
        ax_fire_t.plot(times, fire_comp, color="#111111", lw=1.3, ls="-.", label="Composite")
    ax_fire_t.set_ylabel("Index")
    ax_fire_t.spines["top"].set_visible(False)
    ax_fire.legend(loc="upper left", ncol=2, frameon=False)
    ax_fire_t.legend(loc="upper right", ncol=2, frameon=False)

    ax_fire.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    ax_fire.xaxis.set_major_formatter(plt.FuncFormatter(_format_local_tick))
    axes[-1].set_xlim(times[0], times[-1])

    forecast_hours = data["fhours"]
    title = label.strip() or "Point"
    fig.suptitle(
        f"California Fire Weather HRRR Point Meteogram - {title}",
        fontsize=17,
        fontweight="bold",
        color="#111820",
        y=0.988,
    )
    run_label = f"HRRR {date[:4]}-{date[4:6]}-{date[6:]} {cycle:02d}Z"
    point_label = f"requested {lat:.4f}, {lon:.4f}"
    if math.isfinite(grid_lat) and math.isfinite(grid_lon):
        point_label += f" | nearest grid {grid_lat:.4f}, {grid_lon:.4f}"
    fig.text(
        0.5,
        0.956,
        f"{run_label} | f{min(forecast_hours):03d}-f{max(forecast_hours):03d} | {point_label}",
        ha="center",
        fontsize=11,
        color="#3d4a53",
    )
    fig.text(
        0.5,
        0.014,
        "Experimental model visualization, not an official forecast or evacuation product. Data: NOAA/NCEP HRRR. Rendering: rustwx.",
        ha="center",
        fontsize=9.5,
        color="#65747d",
    )
    fig.subplots_adjust(left=0.065, right=0.93, top=0.93, bottom=0.065)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, facecolor=fig.get_facecolor())
    plt.close(fig)


def render_meteogram_png(
    *,
    settings: Settings,
    report: dict[str, Any],
    label: str,
    force: bool = False,
) -> dict[str, Any]:
    started = time.perf_counter()
    hours = report.get("hours") or []
    if not hours:
        raise RuntimeError("rustwx returned no meteogram hours")

    run = report.get("run") or {}
    date = str(run.get("date_yyyymmdd", "unknown"))
    cycle = int(run.get("cycle_utc", 0))
    point = report.get("point") or {}
    lat = _safe(point.get("lat_deg"))
    lon = _safe(point.get("lon_deg"))
    fhours = [int(hour.get("forecast_hour", 0)) for hour in hours]
    paths = _artifact_paths(
        settings=settings,
        date=date,
        cycle=cycle,
        lat=lat,
        lon=lon,
        forecast_hours=fhours,
        label=label,
    )
    png_key = paths["png_key"]
    json_key = paths["json_key"]
    local_png = paths["local_png"]
    local_json = paths["local_json"]
    cache_hit = local_png.exists() and local_json.exists() and not force

    if not cache_hit:
        _plot(report, label, local_png)
        metadata = {
            "schema_version": 1,
            "artifact": "hrrr_point_meteogram_png",
            "run": run,
            "point": point,
            "label": label,
            "forecast_hours": fhours,
            "source_sample_total_ms": report.get("total_ms"),
            "source_blockers": report.get("blockers", []),
            "png_key": png_key,
        }
        local_json.parent.mkdir(parents=True, exist_ok=True)
        local_json.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    store = ArtifactStore(settings)
    png_url = None
    json_url = None
    if store.enabled():
        png_url = store.upload_file(local_png, png_key, immutable=True)
        json_url = store.upload_file(local_json, json_key, immutable=True)

    if png_url is None:
        png_url = _public_url(settings, png_key)
    if json_url is None:
        json_url = _public_url(settings, json_key)

    return {
        "ok": True,
        "artifact": "hrrr_point_meteogram_png",
        "cache_hit": cache_hit,
        "url": png_url,
        "metadata_url": json_url,
        "key": png_key,
        "metadata_key": json_key,
        "run": run,
        "point": point,
        "label": label,
        "forecast_hours": fhours,
        "sample_total_ms": report.get("total_ms"),
        "render_total_ms": int((time.perf_counter() - started) * 1000),
    }


def cached_meteogram_png(
    *,
    settings: Settings,
    date_yyyymmdd: str,
    cycle_utc: int,
    lat: float,
    lon: float,
    forecast_hours: list[int],
    label: str,
) -> dict[str, Any] | None:
    if not forecast_hours:
        return None
    paths = _artifact_paths(
        settings=settings,
        date=date_yyyymmdd,
        cycle=cycle_utc,
        lat=lat,
        lon=lon,
        forecast_hours=forecast_hours,
        label=label,
    )
    local_png = paths["local_png"]
    local_json = paths["local_json"]
    if not local_png.exists() or not local_json.exists():
        return None
    try:
        metadata = json.loads(local_json.read_text(encoding="utf-8"))
    except Exception:
        metadata = {}
    return {
        "ok": True,
        "artifact": "hrrr_point_meteogram_png",
        "cache_hit": True,
        "url": _public_url(settings, paths["png_key"]),
        "metadata_url": _public_url(settings, paths["json_key"]),
        "key": paths["png_key"],
        "metadata_key": paths["json_key"],
        "run": metadata.get("run")
        or {
            "model": "Hrrr",
            "date_yyyymmdd": date_yyyymmdd,
            "cycle_utc": cycle_utc,
            "source": "Nomads",
            "surface_product": "sfc",
        },
        "point": metadata.get("point") or {"lat_deg": lat, "lon_deg": lon},
        "label": label,
        "forecast_hours": forecast_hours,
        "sample_total_ms": 0,
        "render_total_ms": 0,
    }
