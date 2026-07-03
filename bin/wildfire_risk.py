#!/usr/bin/env python3
"""Layer 5 - Wildfire risk.

Fire-weather risk from the Fosberg Fire Weather Index (FFWI), which combines air
temperature, relative humidity (via fine-fuel equilibrium moisture) and wind
speed, then modulated by *fuel dryness* drawn from the soil-moisture and
forest-water-stress layers:

    risk = FFWI * (0.6 + 0.4 * fuel_dryness)

Daily values use the peak (hottest/driest/windiest) hour of each day.
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

RISK_THRESHOLDS = [20, 40, 60, 75]
RISK_LABELS = ["low", "moderate", "high", "very_high", "extreme"]


def equilibrium_moisture(temp_f, rh):
    """Fosberg fine-fuel equilibrium moisture content (%)."""
    rh = np.clip(rh, 0, 100)
    m = np.where(
        rh < 10,
        0.03229 + 0.281073 * rh - 0.000578 * rh * temp_f,
        np.where(
            rh <= 50,
            2.22749 + 0.160107 * rh - 0.014784 * temp_f,
            21.0606 + 0.005565 * rh ** 2 - 0.00035 * rh * temp_f - 0.483199 * rh,
        ),
    )
    return np.clip(m, 0, 30)


def ffwi(temp_c, rh, wind_ms):
    """Fosberg Fire Weather Index from temperature (C), RH (%), wind (m/s)."""
    temp_f = temp_c * 9.0 / 5.0 + 32.0
    wind_mph = wind_ms * 2.23694
    m = equilibrium_moisture(temp_f, rh)
    mr = m / 30.0
    eta = 1.0 - 2.0 * mr + 1.5 * mr ** 2 - 0.5 * mr ** 3
    return np.clip(eta * np.sqrt(1.0 + wind_mph ** 2) / 0.3002, 0, 100)


def fuel_dryness(sm_layer, fws_layer):
    """Combine region soil moisture and forest water stress into a 0-1 dryness."""
    fwsi = (fws_layer or {}).get("region_mean_fwsi")
    sm = (sm_layer or {}).get("region_mean_current")
    terms, weights = [], []
    if fwsi is not None:
        terms.append(float(fwsi)); weights.append(0.5)
    if sm is not None:
        terms.append(float(np.clip(1.0 - sm / 0.40, 0, 1))); weights.append(0.5)
    if not terms:
        return 0.5
    return float(np.clip(sum(w * t for w, t in zip(weights, terms)) / sum(weights), 0, 1))


def build(obs_paths, sm_layer, fws_layer, config):
    df = dc.load_observations(obs_paths)
    locs = dc.node_locations(df)
    dryness = fuel_dryness(sm_layer, fws_layer)

    # Region-wide hourly wind fallback (many forest nodes lack anemometers).
    region_wind = dc.hourly_series(df, "wind_speed")
    default_wind = float(region_wind.median()) if not region_wind.empty else 2.2

    points, series = [], {}
    for node in sorted(df["node"].unique()):
        ta = dc.hourly_series(df, "air_temp", node=node)
        rh = dc.hourly_series(df, "rel_humidity", node=node)
        if ta.empty or rh.empty:
            continue
        idx = ta.index.intersection(rh.index)
        if len(idx) == 0:
            continue
        ws = dc.hourly_series(df, "wind_speed", node=node)
        wind_assumed = ws.empty
        ws = (region_wind.reindex(idx) if not region_wind.empty
              else pd.Series(default_wind, index=idx)) if ws.empty else ws.reindex(idx)
        ws = ws.fillna(default_wind)

        hourly = pd.Series(
            ffwi(ta.reindex(idx).values, rh.reindex(idx).values, ws.values), index=idx
        )
        daily = hourly.resample("1D").max().dropna()
        if daily.empty:
            continue
        adj = (daily * (0.6 + 0.4 * dryness)).clip(0, 100)

        lat, lon = locs.get(node, (None, None))
        current = float(adj.iloc[-1])
        points.append(
            {
                "node": node,
                "lat": lat,
                "lon": lon,
                "ffwi": round(float(daily.iloc[-1]), 1),
                "risk_index": round(current, 1),
                "period_max": round(float(adj.max()), 1),
                "wind_assumed": bool(wind_assumed),
                "class": dc.classify(current, RISK_THRESHOLDS, RISK_LABELS),
            }
        )
        series[node] = {str(d.date()): round(float(v), 1) for d, v in adj.items()}

    grid = dc.idw_grid([(p["lat"], p["lon"], p["risk_index"]) for p in points],
                       config.get("bbox", {}))
    region = round(float(np.mean([p["risk_index"] for p in points])), 1) if points else None

    return {
        "layer": "wildfire_risk",
        "units": "Fosberg FFWI (0-100), fuel-adjusted",
        "region": config.get("region"),
        "fuel_dryness": round(dryness, 3),
        "n_locations": len(points),
        "region_mean_risk": region,
        "region_class": dc.classify(region, RISK_THRESHOLDS, RISK_LABELS) if region is not None else None,
        "classes": RISK_LABELS,
        "points": points,
        "grid": ({"lats": grid[0], "lons": grid[1], "values": grid[2]} if grid else None),
        "daily_series": series,
    }


def load_json(path):
    if path and os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return None


def main():
    ap = argparse.ArgumentParser(description="Build wildfire risk layer")
    ap.add_argument("--observations", nargs="+", required=True,
                    help="Harmonized (or raw) observation CSV(s)")
    ap.add_argument("--soil-moisture")
    ap.add_argument("--forest-stress")
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    with open(args.config) as fh:
        config = json.load(fh)
    result = build(args.observations,
                   load_json(args.soil_moisture), load_json(args.forest_stress), config)
    with open(args.output, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"wildfire_risk: region risk={result['region_mean_risk']} "
          f"({result['region_class']}), fuel_dryness={result['fuel_dryness']}")


if __name__ == "__main__":
    main()
