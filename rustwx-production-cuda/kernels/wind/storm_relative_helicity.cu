// Storm-relative helicity (positive, negative, total) integrated from the
// surface to `depth` meters AGL. One thread = one column.
// Inputs are (ncols, nlevels) row-major: element (col, k) at col*nlevels + k.
// Mirrors metrust::calc::wind::storm_relative_helicity for monotonic profiles.

extern "C" __global__
void storm_relative_helicity_kernel(
    const double* __restrict__ u,           // (ncols, nlevels) m/s
    const double* __restrict__ v,           // (ncols, nlevels) m/s
    const double* __restrict__ heights,     // (ncols, nlevels) meters AGL
    double storm_u,                          // storm motion u component
    double storm_v,                          // storm motion v component
    double depth,                            // integration depth in meters
    double* __restrict__ srh_pos_out,       // (ncols,) positive SRH
    double* __restrict__ srh_neg_out,       // (ncols,) negative SRH
    double* __restrict__ srh_total_out,     // (ncols,)
    int ncols,
    int nlevels
) {
    int col = blockDim.x * blockIdx.x + threadIdx.x;
    if (col >= ncols) return;
    if (nlevels < 2) {
        srh_pos_out[col] = 0.0;
        srh_neg_out[col] = 0.0;
        srh_total_out[col] = 0.0;
        return;
    }

    double pos = 0.0, neg = 0.0;
    int offset = col * nlevels;

    double h_start = heights[offset];
    double h_end = h_start + depth;
    double prev_h = h_start;
    double prev_u = u[offset];
    double prev_v = v[offset];
    bool integrated = false;

    for (int k = 1; k < nlevels; k++) {
        double curr_h = heights[offset + k];
        double curr_u = u[offset + k];
        double curr_v = v[offset + k];
        if (curr_h <= prev_h) {
            prev_h = curr_h;
            prev_u = curr_u;
            prev_v = curr_v;
            continue;
        }

        double next_h = curr_h;
        double next_u = curr_u;
        double next_v = curr_v;

        if (curr_h >= h_end) {
            double frac = (h_end - prev_h) / (curr_h - prev_h);
            if (frac < 0.0) frac = 0.0;
            if (frac > 1.0) frac = 1.0;
            next_h = h_end;
            next_u = prev_u + frac * (curr_u - prev_u);
            next_v = prev_v + frac * (curr_v - prev_v);
        }

        double sru0 = prev_u - storm_u;
        double srv0 = prev_v - storm_v;
        double sru1 = next_u - storm_u;
        double srv1 = next_v - storm_v;
        double val = (sru1 * srv0) - (sru0 * srv1);

        if (val > 0.0) pos += val;
        else neg += val;
        integrated = true;

        if (curr_h >= h_end) break;
        prev_h = curr_h;
        prev_u = curr_u;
        prev_v = curr_v;
    }

    if (!integrated) {
        pos = 0.0;
        neg = 0.0;
    }

    srh_pos_out[col] = pos;
    srh_neg_out[col] = neg;
    srh_total_out[col] = pos + neg;
}
