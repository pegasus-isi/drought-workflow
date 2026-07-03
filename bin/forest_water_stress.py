#!/usr/bin/env python3
"""Layer 2 - Forest water stress map.

Builds a Forest Water Stress Index (FWSI, 0-1) from the GLEES eddy-flux tower and
surrounding microclimate sensors. Stress rises as the canopy transpires less of
the available energy (low evaporative fraction LE/(LE+H)), as atmospheric demand
rises (high VPD), and as the root zone dries (low soil moisture):

    FWSI = 0.4*(1 - EF) + 0.35*norm(VPD) + 0.25*(1 - norm(SM))

EF needs flux data (GLEES only); where a node lacks fluxes the index falls back
to the VPD/soil-moisture terms, re-weighted.
"""

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
import drought_common as dc  # noqa: E402

FWSI_THRESHOLDS = [0.2, 0.4, 0.6, 0.8]
FWSI_LABELS = ["none", "low", "moderate", "high", "severe"]


def daily_vpd(df, node):
    """Daily-max VPD (kPa) for a node, preferring a measured VPD channel."""
    direct = dc.daily_series(df[df["node"] == node], "vpd", node=node, how="max")
    if not direct.empty:
        return direct
    ta = dc.hourly_series(df, "air_temp", node=node)
    rh = dc.hourly_series(df, "rel_humidity", node=node)
    if ta.empty or rh.empty:
        return ta.iloc[0:0]
    vpd = dc.vpd_kpa(ta, rh.reindex(ta.index))
    return vpd.dropna().resample("1D").max().dropna()


def daily_ef(df, node):
    """Daytime daily-mean evaporative fraction for a node (needs LE & H)."""
    le = dc.hourly_series(df, "le", node=node)
    h = dc.hourly_series(df, "h", node=node)
    if le.empty or h.empty:
        return le.iloc[0:0]
    nr = dc.hourly_series(df, "netrad", node=node)
    idx = le.index.intersection(h.index)
    le, h = le.reindex(idx), h.reindex(idx)
    if not nr.empty:
        nr = nr.reindex(idx)
        day = nr > 50  # daytime only
        le, h = le[day], h[day]
    ef = dc.evaporative_fraction(le.values, h.values)
    out = (
        np.nan_to_num(ef, nan=np.nan)
    )
    import pandas as pd

    return pd.Series(out, index=le.index).dropna().resample("1D").mean().dropna()


def build(obs_paths, config):
    import pandas as pd

    df = dc.load_observations(obs_paths)
    locs = dc.node_locations(df)
    nodes = sorted(df["node"].unique())

    points, series = [], {}
    for node in nodes:
        vpd = daily_vpd(df, node)
        sm = dc.daily_series(df[df["node"] == node], "soil_moisture", node=node)
        ef = daily_ef(df, node)
        if vpd.empty and sm.empty and ef.empty:
            continue

        idx = vpd.index
        for s in (sm, ef):
            idx = idx.union(s.index) if len(idx) else s.index
        if len(idx) == 0:
            continue

        vpd_n = dc.normalize01(vpd.reindex(idx)) if not vpd.empty else None
        sm_n = dc.normalize01(sm.reindex(idx)) if not sm.empty else None
        ef_r = ef.reindex(idx) if not ef.empty else None

        # Re-weight to the terms actually available at this node.
        terms, weights = [], []
        if ef_r is not None:
            terms.append(1.0 - ef_r.fillna(ef_r.mean()))
            weights.append(0.40)
        if vpd_n is not None:
            terms.append(vpd_n.fillna(vpd_n.mean()))
            weights.append(0.35)
        if sm_n is not None:
            terms.append((1.0 - sm_n).fillna((1.0 - sm_n).mean()))
            weights.append(0.25)
        wsum = sum(weights)
        fwsi = sum(w * t for w, t in zip(weights, terms)) / wsum
        fwsi = fwsi.clip(0, 1)

        lat, lon = locs.get(node, (None, None))
        current = float(fwsi.iloc[-1])
        points.append(
            {
                "node": node,
                "lat": lat,
                "lon": lon,
                "fwsi": round(current, 3),
                "period_mean": round(float(fwsi.mean()), 3),
                "has_flux": ef_r is not None,
                "class": dc.classify(current, FWSI_THRESHOLDS, FWSI_LABELS),
            }
        )
        series[node] = {str(d.date()): round(float(v), 3) for d, v in fwsi.items()}

    grid = dc.idw_grid([(p["lat"], p["lon"], p["fwsi"]) for p in points],
                       config.get("bbox", {}))
    region = (round(float(np.mean([p["fwsi"] for p in points])), 3)
              if points else None)

    return {
        "layer": "forest_water_stress",
        "units": "FWSI 0-1 (higher = more stressed)",
        "region": config.get("region"),
        "formula": "0.4*(1-EF) + 0.35*norm(VPD) + 0.25*(1-norm(SM))",
        "n_locations": len(points),
        "region_mean_fwsi": region,
        "classes": FWSI_LABELS,
        "points": points,
        "grid": ({"lats": grid[0], "lons": grid[1], "values": grid[2]}
                 if grid else None),
        "daily_series": series,
    }


def main():
    ap = argparse.ArgumentParser(description="Build forest water stress layer")
    ap.add_argument("--observations", nargs="+", required=True,
                    help="Harmonized (or raw) observation CSV(s)")
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    with open(args.config) as fh:
        config = json.load(fh)
    result = build(args.observations, config)
    with open(args.output, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"forest_water_stress: {result['n_locations']} locations, "
          f"region FWSI {result['region_mean_fwsi']}")


if __name__ == "__main__":
    main()
