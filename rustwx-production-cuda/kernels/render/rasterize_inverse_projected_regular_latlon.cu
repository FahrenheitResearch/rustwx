// Inverse-projecting rasterizer for regular lat/lon data grids.
//
// One thread per output pixel:
//   1) Map (px, py) -> projected (x, y) using canvas extent.
//   2) Unproject (x, y) -> (lat, lon) using projection params.
//   3) (Optional) clip-bounds reject in geographic space.
//   4) Bilinear-sample the regular lat/lon grid at (lat, lon).
//   5) Colormap lookup (binary search on levels).
//   6) Write packed RGBA8 (R | G<<8 | B<<16 | A<<24) to the canvas.
//
// f64 throughout — geographic math, precision matters. Matches the CPU
// reference (`rasterize_inverse_projected_grid` in
// crates/rustwx-render/src/rasterize.rs) within a handful of f64 ULPs.

// constants.cuh (prepended by `with_constants`) provides M_PI.

__device__ const double IPRL_R_EARTH = 6370000.0;
__device__ const double IPRL_DEG2RAD = M_PI / 180.0;
__device__ const double IPRL_RAD2DEG = 180.0 / M_PI;

// Robinson lookup table — used only by projection_kind == 1.
__device__ __constant__ double IPRL_ROBINSON_X[19] = {
    1.0000, 0.9986, 0.9954, 0.9900, 0.9822, 0.9730, 0.9600, 0.9427, 0.9216, 0.8962, 0.8679, 0.8350,
    0.7986, 0.7597, 0.7186, 0.6732, 0.6213, 0.5722, 0.5322
};

__device__ __constant__ double IPRL_ROBINSON_Y[19] = {
    0.0000, 0.0620, 0.1240, 0.1860, 0.2480, 0.3100, 0.3720, 0.4340, 0.4958, 0.5571, 0.6176, 0.6769,
    0.7346, 0.7903, 0.8435, 0.8936, 0.9394, 0.9761, 1.0000
};

// Mirror of `stabilize_latitude` in projection.rs.
__device__ __forceinline__ double iprl_stabilize_latitude(double lat_deg) {
    return fmin(89.999, fmax(-89.999, lat_deg));
}

// Mirror of `normalize_longitude_deg` in projection.rs (the projection-space
// version that uses `%` and the > 180 / <= -180 thresholds).
__device__ __forceinline__ double iprl_normalize_projection_lon(double lon_deg) {
    double lon = fmod(lon_deg, 360.0);
    if (lon > 180.0) lon -= 360.0;
    else if (lon <= -180.0) lon += 360.0;
    return lon;
}

// Mirror of the `normalize_longitude_deg` defined in rasterize.rs (note:
// the rasterize-local one uses `>=` 180.0, which differs from projection.rs).
// Used by the regular-latlon axis adjustment.
__device__ __forceinline__ double iprl_normalize_axis_lon(double lon_deg) {
    while (lon_deg < -180.0) lon_deg += 360.0;
    while (lon_deg >= 180.0) lon_deg -= 360.0;
    return lon_deg;
}

// Positive remainder-euclid for periodic-lon grids.
__device__ __forceinline__ double iprl_rem_euclid(double value, double modulus) {
    double r = fmod(value, modulus);
    if (r < 0.0) r += modulus;
    return r;
}

// Geographic clip-bounds containment, mirroring `GeographicClipBounds::contains`.
__device__ __forceinline__ bool iprl_clip_contains(
    int has_clip,
    double west_deg,
    double east_deg,
    double south_deg,
    double north_deg,
    double lat_deg,
    double lon_deg
) {
    if (!has_clip) return true;
    if (!isfinite(lat_deg) || !isfinite(lon_deg)) return false;
    if (lat_deg < south_deg || lat_deg > north_deg) return false;

    // Wide spans (effectively global): always contain in lon.
    double raw_span = fabs(east_deg - west_deg);
    double span;
    if (raw_span >= 359.0) {
        span = fmin(raw_span, 360.0);
    } else {
        double w = iprl_normalize_projection_lon(west_deg);
        double e = iprl_normalize_projection_lon(east_deg);
        span = (w <= e) ? (e - w) : (e + 360.0 - w);
    }
    if (span >= 359.0) return true;

    double w = iprl_normalize_projection_lon(west_deg);
    double e = iprl_normalize_projection_lon(east_deg);
    double lon = iprl_normalize_projection_lon(lon_deg);
    if (w <= e) return lon >= w && lon <= e;
    return lon >= w || lon <= e;
}

// (x, y) -> (lat, lon), packing the upstream projection variants by `kind`:
//   0 = Geographic   (p0 = central_meridian_deg)
//   1 = Robinson     (p0 = central_meridian_deg)
//   2 = AlbersEqualArea (p0=n, p1=c, p2=rho0, p3=central_meridian_deg)
//   3 = LambertConformal (p0=n, p1=f, p2=rho0, p3=stand_lon_deg)
//   4 = Mercator     (p0=central_meridian_deg, p1=scale)
//
// Returns false on out-of-domain or non-finite inputs (caller skips the pixel).
__device__ __forceinline__ bool iprl_unproject(
    int kind,
    double p0, double p1, double p2, double p3, double p4, double p5,
    double x,
    double y,
    double* lat_out,
    double* lon_out
) {
    (void)p4;
    (void)p5;

    if (!isfinite(x) || !isfinite(y)) return false;

    if (kind == 0) {
        // Geographic: y is latitude (clamped), x is longitude offset from
        // central meridian. Mirrors GeographicProjection::unproject.
        *lat_out = iprl_stabilize_latitude(y);
        *lon_out = iprl_normalize_projection_lon(x + p0);
        return true;
    }

    if (kind == 1) {
        // Robinson: piecewise-linear table inversion in y; linear in x.
        double scaled_y = fabs(y / (IPRL_R_EARTH * 1.3523));
        if (scaled_y > 1.0 + 1.0e-9) return false;
        int band = 17;
        for (int idx = 0; idx < 18; ++idx) {
            if (scaled_y <= IPRL_ROBINSON_Y[idx + 1] + 1.0e-12) {
                band = idx;
                break;
            }
        }
        double y0 = IPRL_ROBINSON_Y[band];
        double y1 = IPRL_ROBINSON_Y[band + 1];
        double t = 0.0;
        if (fabs(y1 - y0) > 1.0e-12) {
            t = fmin(1.0, fmax(0.0, (scaled_y - y0) / (y1 - y0)));
        }
        double lat_abs = ((double)band + t) * 5.0;
        double x_scale = IPRL_ROBINSON_X[band] + (IPRL_ROBINSON_X[band + 1] - IPRL_ROBINSON_X[band]) * t;
        if (x_scale <= 0.0) return false;
        double lon_delta = x / (IPRL_R_EARTH * 0.8487 * x_scale) * IPRL_RAD2DEG;
        if (fabs(lon_delta) > 180.0 + 1.0e-6) return false;
        *lat_out = copysign(lat_abs, y);
        *lon_out = iprl_normalize_projection_lon(p0 + lon_delta);
        return true;
    }

    if (kind == 2) {
        // Albers equal area: p0=n, p1=c, p2=rho0, p3=central meridian deg.
        double n = p0;
        double c = p1;
        double rho0 = p2;
        double dy = rho0 - y;
        double rho_abs = sqrt(x * x + dy * dy);
        if (!isfinite(rho_abs) || rho_abs <= 0.0 || fabs(n) < 1.0e-12) return false;
        double rho_sign = (n < 0.0) ? -1.0 : 1.0;
        double rho = rho_abs * rho_sign;
        double theta = atan2(x * rho_sign, dy * rho_sign);
        double rn = rho * n / IPRL_R_EARTH;
        double arg = (c - rn * rn) / (2.0 * n);
        if (!isfinite(arg) || arg < -1.0 || arg > 1.0) return false;
        double lat = asin(arg) * IPRL_RAD2DEG;
        double lon = p3 + theta / n * IPRL_RAD2DEG;
        *lat_out = lat;
        *lon_out = iprl_normalize_projection_lon(lon);
        return true;
    }

    if (kind == 3) {
        // Lambert conformal conic: p0=n, p1=f, p2=rho0, p3=stand_lon_deg.
        double n = p0;
        double f = p1;
        double rho0 = p2;
        double dy = rho0 - y;
        double rho_abs = sqrt(x * x + dy * dy);
        if (!isfinite(rho_abs) || rho_abs <= 0.0 || fabs(n) < 1.0e-12 || fabs(f) < 1.0e-12) return false;
        double rho_sign = (n < 0.0) ? -1.0 : 1.0;
        double rho = rho_abs * rho_sign;
        double theta = atan2(x * rho_sign, dy * rho_sign);
        double ratio = IPRL_R_EARTH * f / rho;
        if (ratio <= 0.0 || !isfinite(ratio)) return false;
        double phi = 2.0 * atan(pow(ratio, 1.0 / n)) - M_PI / 2.0;
        double lon = p3 + theta / n * IPRL_RAD2DEG;
        *lat_out = phi * IPRL_RAD2DEG;
        *lon_out = iprl_normalize_projection_lon(lon);
        return true;
    }

    if (kind == 4) {
        // Mercator: p0=central_meridian_deg, p1=scale (cos(lat_ts), >0).
        double cm = p0;
        double scale = p1;
        if (scale <= 0.0) return false;
        double lon = cm + x / (IPRL_R_EARTH * scale) * IPRL_RAD2DEG;
        double lat = (2.0 * atan(exp(y / (IPRL_R_EARTH * scale))) - M_PI / 2.0) * IPRL_RAD2DEG;
        *lat_out = iprl_stabilize_latitude(lat);
        *lon_out = iprl_normalize_projection_lon(lon);
        return true;
    }

    // Unsupported kind (e.g. PolarStereographic — no analytic inverse here).
    return false;
}

// Translate a longitude into fractional grid x. Mirrors `grid_x_for_axis_lon`
// in rasterize.rs.
__device__ __forceinline__ bool iprl_grid_x_for_axis_lon(
    double lon,
    int nx,
    double lon0,
    double lon_step,
    int periodic_lon,
    double period_points,
    double* gx_out
) {
    if (periodic_lon) {
        *gx_out = iprl_rem_euclid((lon - lon0) / lon_step, period_points);
        return true;
    }
    double adjusted = iprl_normalize_axis_lon(lon);
    double axis_center = lon0 + lon_step * (double)(nx - 1) / 2.0;
    while (adjusted - axis_center > 180.0) adjusted -= 360.0;
    while (adjusted - axis_center < -180.0) adjusted += 360.0;

    double gx = (adjusted - lon0) / lon_step;
    if (gx < 0.0 || gx > (double)(nx - 1)) return false;
    *gx_out = gx;
    return true;
}

// Bilinear with NaN fallback to first finite — matches `bilinear` in rasterize.rs.
__device__ __forceinline__ double iprl_bilinear(
    double v00, double v10, double v01, double v11, double fx, double fy
) {
    if (isfinite(v00) && isfinite(v10) && isfinite(v01) && isfinite(v11)) {
        double south = v00 * (1.0 - fx) + v10 * fx;
        double north = v01 * (1.0 - fx) + v11 * fx;
        return south * (1.0 - fy) + north * fy;
    }
    if (isfinite(v00)) return v00;
    if (isfinite(v10)) return v10;
    if (isfinite(v01)) return v01;
    if (isfinite(v11)) return v11;
    return nan("");
}

// Sample the regular lat/lon grid at (lat, lon). Mirrors
// `sample_regular_latlon_grid` in rasterize.rs.
__device__ __forceinline__ bool iprl_sample_regular_latlon(
    const double* __restrict__ data,
    int ny,
    int nx,
    double lat0,
    double lat_step,
    double lon0,
    double lon_step,
    int periodic_lon,
    double period_points,
    double lat,
    double lon,
    double* value_out
) {
    if (!isfinite(lat) || !isfinite(lon)) return false;
    double gy = (lat - lat0) / lat_step;
    if (gy < 0.0 || gy > (double)(ny - 1)) return false;

    double gx = 0.0;
    if (!iprl_grid_x_for_axis_lon(lon, nx, lon0, lon_step, periodic_lon, period_points, &gx)) {
        return false;
    }

    int i0 = (int)floor(gx);
    if (i0 < 0) i0 = 0;
    if (i0 > nx - 1) i0 = nx - 1;
    int j0 = (int)floor(gy);
    if (j0 < 0) j0 = 0;
    if (j0 > ny - 1) j0 = ny - 1;

    int i1;
    if (periodic_lon) {
        i1 = (int)iprl_rem_euclid((double)(i0 + 1), period_points);
    } else {
        i1 = i0 + 1;
        if (i1 > nx - 1) i1 = nx - 1;
    }
    int j1 = j0 + 1;
    if (j1 > ny - 1) j1 = ny - 1;

    double fx = gx - (double)i0;
    double fy = gy - (double)j0;

    size_t idx00 = (size_t)j0 * (size_t)nx + (size_t)i0;
    size_t idx10 = (size_t)j0 * (size_t)nx + (size_t)i1;
    size_t idx01 = (size_t)j1 * (size_t)nx + (size_t)i0;
    size_t idx11 = (size_t)j1 * (size_t)nx + (size_t)i1;
    *value_out = iprl_bilinear(
        data[idx00], data[idx10], data[idx01], data[idx11], fx, fy
    );
    return true;
}

// Binary-search the levels table -> packed RGBA u32. Same shape as the
// `colormap_lookup` device helper in rasterize_grid.cu / rasterize_projected_grid.cu.
__device__ __forceinline__ unsigned int iprl_colormap_lookup(
    double value,
    const double* __restrict__ levels,
    int n_levels,
    const unsigned int* __restrict__ colors,
    int n_intervals,
    int has_under, unsigned int under_color,
    int has_over,  unsigned int over_color,
    int has_mask_below, double mask_below
) {
    if (isnan(value)) return 0u;
    if (has_mask_below && value < mask_below) return 0u;
    if (n_levels < 2 || n_intervals < 1) return 0u;
    if (value < levels[0]) {
        return has_under ? under_color : 0u;
    }
    // partition_point(|l| *l <= value)
    int lo = 0;
    int hi = n_levels;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        if (levels[mid] <= value) lo = mid + 1;
        else hi = mid;
    }
    int idx = lo;
    if (idx <= n_intervals) {
        int ci = idx - 1;
        if (ci < 0) ci = 0;
        if (ci > n_intervals - 1) ci = n_intervals - 1;
        return colors[ci];
    }
    return has_over ? over_color : colors[n_intervals - 1];
}

extern "C" __global__
void rasterize_inverse_projected_regular_latlon_kernel(
    const double* __restrict__ data,
    int ny,
    int nx,
    double lat0,
    double lat_step,
    double lon0,
    double lon_step,
    int periodic_lon,
    double period_points,
    int projection_kind,
    double p0,
    double p1,
    double p2,
    double p3,
    double p4,
    double p5,
    int has_clip,
    double clip_west,
    double clip_east,
    double clip_south,
    double clip_north,
    double x_min,
    double x_max,
    double y_min,
    double y_max,
    const double* __restrict__ levels,
    int n_levels,
    const unsigned int* __restrict__ colors,
    int n_intervals,
    int has_under,
    unsigned int under_color,
    int has_over,
    unsigned int over_color,
    int has_mask_below,
    double mask_below,
    unsigned int* __restrict__ out,
    int img_w,
    int img_h
) {
    int px = blockIdx.x * blockDim.x + threadIdx.x;
    int py = blockIdx.y * blockDim.y + threadIdx.y;
    if (px >= img_w || py >= img_h) return;
    if (ny < 2 || nx < 2) return;

    // Map output pixel -> projected (x, y). Mirror the CPU loop:
    //   y goes top-down (py=0 -> y_max), x goes left-right (px=0 -> x_min).
    double x_den = (double)((img_w > 1) ? (img_w - 1) : 1);
    double y_den = (double)((img_h > 1) ? (img_h - 1) : 1);
    double x = x_min + ((double)px / x_den) * (x_max - x_min);
    double y = y_max - ((double)py / y_den) * (y_max - y_min);

    double lat = 0.0;
    double lon = 0.0;
    if (!iprl_unproject(projection_kind, p0, p1, p2, p3, p4, p5, x, y, &lat, &lon)) return;
    if (!iprl_clip_contains(has_clip, clip_west, clip_east, clip_south, clip_north, lat, lon)) return;

    double value = 0.0;
    if (!iprl_sample_regular_latlon(
            data, ny, nx, lat0, lat_step, lon0, lon_step,
            periodic_lon, period_points, lat, lon, &value
        )) {
        return;
    }

    unsigned int rgba = iprl_colormap_lookup(
        value, levels, n_levels, colors, n_intervals,
        has_under, under_color, has_over, over_color,
        has_mask_below, mask_below
    );
    if (((rgba >> 24) & 0xFFu) == 0u) return;
    out[(size_t)py * (size_t)img_w + (size_t)px] = rgba;
}
