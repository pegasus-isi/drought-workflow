#!/usr/bin/env python3
"""Layer 1 - Soil moisture map.

Combines volumetric soil water content from GLEES (SWC) and any Sage soil probes
into per-location current values, period statistics, and an optional IDW grid.
Each location is classified on a dryness scale used downstream by the drought and
wildfire layers.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
import drought_common as dc  # noqa: E402

# Volumetric water content (fraction) breakpoints -> class.
SM_THRESHOLDS = [0.10, 0.20, 0.30, 0.40]
SM_LABELS = ["very_dry", "dry", "moderate", "moist", "saturated"]


def build(obs_paths, config):
    df = dc.load_observations(obs_paths)
    bbox = config.get("bbox", {})
    locs = dc.node_locations(df)

    sm = df[df["variable"] == "soil_moisture"]
    points = []
    series = {}
    for node, grp in sm.groupby("node"):
        daily = dc.daily_series(grp, "soil_moisture", node=node)
        if daily.empty:
            continue
        lat, lon = locs.get(node, (None, None))
        current = float(daily.iloc[-1])
        points.append(
            {
                "node": node,
                "lat": lat,
                "lon": lon,
                "current": round(current, 4),
                "period_mean": round(float(daily.mean()), 4),
                "period_min": round(float(daily.min()), 4),
                "anomaly": round(current - float(daily.mean()), 4),
                "class": dc.classify(current, SM_THRESHOLDS, SM_LABELS),
            }
        )
        series[node] = {
            str(d.date()): round(float(v), 4) for d, v in daily.items()
        }

    grid = dc.idw_grid(
        [(p["lat"], p["lon"], p["current"]) for p in points], bbox
    )

    region_current = (
        round(float(sum(p["current"] for p in points) / len(points)), 4)
        if points
        else None
    )

    return {
        "layer": "soil_moisture_map",
        "units": "m3/m3 (volumetric fraction)",
        "region": config.get("region"),
        "n_locations": len(points),
        "region_mean_current": region_current,
        "classes": SM_LABELS,
        "points": points,
        "grid": (
            {"lats": grid[0], "lons": grid[1], "values": grid[2]}
            if grid
            else None
        ),
        "daily_series": series,
    }


def main():
    ap = argparse.ArgumentParser(description="Build soil moisture map layer")
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
    print(f"soil_moisture_map: {result['n_locations']} locations, "
          f"region mean {result['region_mean_current']}")


if __name__ == "__main__":
    main()
