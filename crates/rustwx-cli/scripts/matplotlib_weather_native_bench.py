import argparse
import json
import statistics
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser(
        description="Render a weather-native benchmark payload with matplotlib/cartopy."
    )
    parser.add_argument("--payload", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--runs", type=int, default=5)
    return parser.parse_args()


def parse_projection(payload_projection):
    if payload_projection is None or payload_projection == "geographic":
        return ccrs.PlateCarree()
    if isinstance(payload_projection, dict):
        if "lambert_conformal" in payload_projection:
            params = payload_projection["lambert_conformal"]
            return ccrs.LambertConformal(
                central_longitude=params["central_meridian_deg"],
                standard_parallels=(
                    params["standard_parallel_1_deg"],
                    params["standard_parallel_2_deg"],
                ),
            )
        if "mercator" in payload_projection:
            params = payload_projection["mercator"]
            return ccrs.Mercator(
                central_longitude=params["central_meridian_deg"],
                latitude_true_scale=params["latitude_of_true_scale_deg"],
            )
        if "polar_stereographic" in payload_projection:
            params = payload_projection["polar_stereographic"]
            central_latitude = -90.0 if params["south_pole_on_projection_plane"] else 90.0
            return ccrs.Stereographic(
                central_latitude=central_latitude,
                central_longitude=params["central_meridian_deg"],
                true_scale_latitude=params["true_latitude_deg"],
            )
        if "other" in payload_projection:
            return ccrs.PlateCarree()
    return ccrs.PlateCarree()


def extend_mode(mode):
    normalized = str(mode).strip().lower()
    if normalized == "min":
        return "min"
    if normalized == "max":
        return "max"
    if normalized == "both":
        return "both"
    return "neither"


def to_rgba(color):
    return (
        color["r"] / 255.0,
        color["g"] / 255.0,
        color["b"] / 255.0,
        color["a"] / 255.0,
    )


def build_color_objects(scale):
    colors = [to_rgba(color) for color in scale["colors"]]
    cmap = mcolors.ListedColormap(colors)
    if colors:
        cmap.set_under(colors[0])
        cmap.set_over(colors[-1])
    norm = mcolors.BoundaryNorm(scale["levels"], cmap.N, extend=extend_mode(scale["extend"]))
    return cmap, norm


def preload_features():
    coastlines = cfeature.NaturalEarthFeature(
        "physical", "coastline", "110m", facecolor="none"
    )
    states = cfeature.NaturalEarthFeature(
        "cultural", "admin_1_states_provinces_lines", "110m", facecolor="none"
    )
    list(coastlines.geometries())
    list(states.geometries())
    return coastlines, states


def render_once(
    payload,
    projection,
    plate_carree,
    coastlines,
    states,
    output_path,
):
    width = payload["width"]
    height = payload["height"]
    dpi = 100.0
    lon = np.asarray(payload["lon_deg"], dtype=np.float32).reshape(payload["ny"], payload["nx"])
    lat = np.asarray(payload["lat_deg"], dtype=np.float32).reshape(payload["ny"], payload["nx"])
    field = np.asarray(payload["values"], dtype=np.float32).reshape(payload["ny"], payload["nx"])
    field = np.ma.masked_invalid(field)
    mask_below = payload["scale"].get("mask_below")
    if mask_below is not None:
        field = np.ma.masked_where(field < mask_below, field)

    cmap, norm = build_color_objects(payload["scale"])
    bounds = payload["bounds"]

    fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi, facecolor="white")
    ax = fig.add_axes([0.05, 0.17, 0.90, 0.73], projection=projection)
    ax.set_extent([bounds[0], bounds[1], bounds[2], bounds[3]], crs=plate_carree)
    ax.add_feature(
        coastlines, edgecolor=(0.15, 0.15, 0.15, 0.85), linewidth=0.5, zorder=5
    )
    ax.add_feature(states, edgecolor=(0.20, 0.20, 0.20, 0.55), linewidth=0.35, zorder=5)
    filled = ax.contourf(
        lon,
        lat,
        field,
        levels=payload["scale"]["levels"],
        cmap=cmap,
        norm=norm,
        extend=extend_mode(payload["scale"]["extend"]),
        transform=plate_carree,
        antialiased=False,
        zorder=2,
    )
    if payload["line_levels"]:
        ax.contour(
            lon,
            lat,
            field,
            levels=payload["line_levels"],
            colors=[(0.12, 0.07, 0.16, 0.85)],
            linewidths=0.7,
            transform=plate_carree,
            zorder=4,
        )
    cax = fig.add_axes([0.12, 0.08, 0.76, 0.028])
    fig.colorbar(filled, cax=cax, orientation="horizontal")
    fig.text(0.5, 0.96, payload["title"], ha="center", va="top", fontsize=15, weight="bold")
    fig.text(
        0.5,
        0.93,
        f'{payload["recipe_slug"]} | matplotlib/cartopy equivalent',
        ha="center",
        va="top",
        fontsize=9,
        color=(0.20, 0.20, 0.20),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def main():
    args = parse_args()
    payload_path = Path(args.payload)
    output_path = Path(args.output)
    summary_path = Path(args.summary)
    setup_start = time.perf_counter()
    with payload_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    projection = parse_projection(payload.get("projection"))
    plate_carree = ccrs.PlateCarree()
    coastlines, states = preload_features()
    setup_ms = (time.perf_counter() - setup_start) * 1000.0

    render_once(payload, projection, plate_carree, coastlines, states, output_path)

    run_count = max(1, args.runs)
    render_save_ms_runs = []
    for _ in range(run_count):
        start = time.perf_counter()
        render_once(payload, projection, plate_carree, coastlines, states, output_path)
        render_save_ms_runs.append((time.perf_counter() - start) * 1000.0)

    summary = {
        "setup_ms": setup_ms,
        "render_save_ms_runs": render_save_ms_runs,
        "median_render_save_ms": statistics.median(render_save_ms_runs),
        "output_png": str(output_path.resolve()),
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)


if __name__ == "__main__":
    main()
