#!/usr/bin/env python3
"""Layer 4 - Drought warning.

A composite drought severity index (DSI, 0-1) blended from the upstream soil
moisture and forest-water-stress layers plus precipitation and atmospheric
demand from the raw observations:

    DSI = 0.35*(1 - soil_moisture_percentile)
        + 0.25*forest_water_stress
        + 0.20*precip_deficit
        + 0.20*norm(VPD)

The current DSI is mapped to USDM-style categories (None, D0-D4) and a warning
level. A daily DSI series captures the trend.
"""

import argparse
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
import drought_common as dc  # noqa: E402

# DSI breakpoints -> USDM-style drought categories.
DSI_THRESHOLDS = [0.20, 0.40, 0.60, 0.80, 0.90]
DSI_LABELS = ["None", "D0 (Abnormally Dry)", "D1 (Moderate)",
              "D2 (Severe)", "D3 (Extreme)", "D4 (Exceptional)"]
WARNING = {
    "None": "normal", "D0 (Abnormally Dry)": "watch", "D1 (Moderate)": "warning",
    "D2 (Severe)": "warning", "D3 (Extreme)": "emergency",
    "D4 (Exceptional)": "emergency",
}


def region_daily_from_layer(layer_json, key):
    """Average a layer's per-node daily_series into one region daily Series."""
    if not layer_json:
        return pd.Series(dtype=float)
    series = layer_json.get("daily_series", {})
    frames = []
    for node, vals in series.items():
        if isinstance(vals, dict) and vals and not isinstance(
            next(iter(vals.values())), dict
        ):
            s = pd.Series(vals)
            s.index = pd.to_datetime(s.index, utc=True)
            frames.append(s)
    if not frames:
        return pd.Series(dtype=float)
    return pd.concat(frames, axis=1).mean(axis=1).sort_index()


def build(obs_paths, sm_layer, fws_layer, config):
    df = dc.load_observations(obs_paths)

    # Region soil moisture (daily mean across all soil probes).
    sm = (
        df[df["variable"] == "soil_moisture"]
        .set_index("timestamp")["value"]
        .resample("1D").mean().dropna()
    )
    sm_pctl = sm.rank(pct=True) if not sm.empty else sm  # 0 driest .. 1 wettest

    # Atmospheric demand: daily-max VPD (measured or derived).
    vpd = df[df["variable"] == "vpd"].set_index("timestamp")["value"].resample("1D").max().dropna()
    if vpd.empty:
        ta = df[df["variable"] == "air_temp"].set_index("timestamp")["value"].resample("1h").mean()
        rh = df[df["variable"] == "rel_humidity"].set_index("timestamp")["value"].resample("1h").mean()
        if not ta.empty and not rh.empty:
            v = dc.vpd_kpa(ta, rh.reindex(ta.index)).dropna()
            vpd = v.resample("1D").max().dropna()
    vpd_n = dc.normalize01(vpd) if not vpd.empty else pd.Series(dtype=float)

    # Precipitation deficit from 14-day rolling totals (1 = driest).
    precip = df[df["variable"] == "precip"].set_index("timestamp")["value"].resample("1D").sum()
    if not precip.empty:
        roll = precip.rolling(14, min_periods=3).sum().dropna()
        precip_def = 1.0 - dc.normalize01(roll)
    else:
        precip_def = pd.Series(dtype=float)

    fwsi = region_daily_from_layer(fws_layer, "fwsi")

    # Align everything on the soil-moisture daily index (the backbone series).
    idx = sm.index
    for s in (vpd_n, precip_def, fwsi):
        if not s.empty and len(idx):
            idx = idx.union(s.index)
    if len(idx) == 0:
        idx = pd.DatetimeIndex([], tz="UTC")

    def term(s, default):
        return s.reindex(idx).interpolate().bfill().ffill() if not s.empty else pd.Series(default, index=idx)

    dsi = (
        0.35 * (1.0 - term(sm_pctl, 0.5))
        + 0.25 * term(fwsi, 0.0)
        + 0.20 * term(precip_def, 0.5)
        + 0.20 * term(vpd_n, 0.5)
    ).clip(0, 1)

    current = float(dsi.iloc[-1]) if not dsi.empty else None
    category = dc.classify(current, DSI_THRESHOLDS, DSI_LABELS) if current is not None else "unknown"

    components = {
        "soil_moisture_dryness": round(float(1.0 - term(sm_pctl, 0.5).iloc[-1]), 3) if len(idx) else None,
        "forest_water_stress": round(float(term(fwsi, 0.0).iloc[-1]), 3) if len(idx) else None,
        "precip_deficit": round(float(term(precip_def, 0.5).iloc[-1]), 3) if len(idx) else None,
        "vpd_demand": round(float(term(vpd_n, 0.5).iloc[-1]), 3) if len(idx) else None,
    }

    trend = None
    if not dsi.empty and len(dsi) >= 7:
        trend = round(float(dsi.iloc[-1] - dsi.iloc[-7]), 3)

    return {
        "layer": "drought_warning",
        "region": config.get("region"),
        "current_dsi": round(current, 3) if current is not None else None,
        "category": category,
        "warning_level": WARNING.get(category, "unknown"),
        "weekly_trend": trend,
        "components": components,
        "categories": DSI_LABELS,
        "daily_dsi": {str(d.date()): round(float(v), 3) for d, v in dsi.items()},
    }


def load_json(path):
    if path and os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return None


def main():
    ap = argparse.ArgumentParser(description="Build drought warning layer")
    ap.add_argument("--observations", nargs="+", required=True,
                    help="Harmonized (or raw) observation CSV(s)")
    ap.add_argument("--soil-moisture", help="soil_moisture_map.json")
    ap.add_argument("--forest-stress", help="forest_water_stress.json")
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    with open(args.config) as fh:
        config = json.load(fh)
    result = build(args.observations,
                   load_json(args.soil_moisture), load_json(args.forest_stress), config)
    with open(args.output, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"drought_warning: DSI={result['current_dsi']} "
          f"category={result['category']} level={result['warning_level']}")


if __name__ == "__main__":
    main()
