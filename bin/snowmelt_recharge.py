#!/usr/bin/env python3
"""Layer 3 - Snowmelt recharge timing.

Tracks the seasonal snowpack at GLEES and the soil's response to melt to estimate
when meltwater recharges the root zone:

  * peak snowpack date          (max snow depth)
  * melt-onset date             (sustained decline from peak)
  * snow-free date              (snow depth ~ 0 after peak)
  * soil-thaw date              (soil temperature crosses 0 degC)
  * recharge-onset date         (first sustained soil-moisture rise after melt)
  * recharge lag                (days from snow-free to soil-moisture peak)
  * recharge magnitude          (soil-moisture gain over the melt pulse)

Primarily a GLEES (point) layer since snow depth comes from the tower.
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


def _date(ts):
    return None if ts is None or pd.isna(ts) else str(pd.Timestamp(ts).date())


def detect(snow, soil_t, soil_m):
    """Derive snowmelt/recharge events from daily series.

    ``snow`` is the melt signal (SWE preferred, else snow depth). Thresholds are
    relative to the seasonal peak so the same logic works for either signal/unit.
    """
    out = {
        "peak_snow_date": None,
        "peak_snow_value": None,
        "melt_onset_date": None,
        "snow_free_date": None,
        "soil_thaw_date": None,
        "recharge_onset_date": None,
        "recharge_lag_days": None,
        "recharge_magnitude": None,
    }

    if not snow.empty:
        snow = snow.rolling(3, min_periods=1, center=True).mean()
        peak_date = snow.idxmax()
        peak = float(snow.max())
        out["peak_snow_date"] = _date(peak_date)
        out["peak_snow_value"] = round(peak, 3)

        after = snow[snow.index >= peak_date]
        # Melt onset: first day after peak losing >1% of peak/day for 3+ days.
        decline = after.diff() < -0.01 * peak
        run = decline.rolling(3).sum()
        onset = run[run >= 3].index.min() if (run >= 3).any() else None
        out["melt_onset_date"] = _date(onset)

        snow_free = after[after <= 0.05 * peak]
        sf_date = snow_free.index.min() if not snow_free.empty else None
        out["snow_free_date"] = _date(sf_date)
    else:
        sf_date = None

    if not soil_t.empty:
        thawed = soil_t[soil_t > 0.0]
        out["soil_thaw_date"] = _date(thawed.index.min() if not thawed.empty else None)

    if not soil_m.empty:
        sm = soil_m.rolling(3, min_periods=1, center=True).mean()
        # Look at the post-peak-snow / spring window for the recharge pulse.
        ref = pd.Timestamp(out["peak_snow_date"], tz="UTC") if out["peak_snow_date"] else sm.index.min()
        window = sm[sm.index >= ref]
        if len(window) > 5:
            rise = window.diff() > 0.005
            run = rise.rolling(3).sum()
            r_onset = run[run >= 3].index.min() if (run >= 3).any() else None
            out["recharge_onset_date"] = _date(r_onset)
            peak_sm_date = window.idxmax()
            base = float(window.iloc[0])
            out["recharge_magnitude"] = round(float(window.max() - base), 4)
            if sf_date is not None and peak_sm_date is not None:
                out["recharge_lag_days"] = int((peak_sm_date - sf_date).days)
    return out


def _best_snow_node(df, variable):
    """Node with the most observations of a snow variable, or None."""
    sub = df[df["variable"] == variable]
    if sub.empty:
        return None
    return sub.groupby("node").size().idxmax()


def build(obs_paths, config):
    df = dc.load_observations(obs_paths)
    locs = dc.node_locations(df)

    # Prefer SWE (best recharge driver) from whichever node reports it, else
    # fall back to snow depth. Snow now typically comes from SNOTEL #367.
    signal_var = "swe" if not df[df["variable"] == "swe"].empty else "snow_depth"
    snow_node = _best_snow_node(df, signal_var)

    snow = dc.daily_series(df, signal_var, node=snow_node)
    snow_depth = dc.daily_series(df, "snow_depth", node=_best_snow_node(df, "snow_depth"))

    # Soil from the snow node when available, else the region (any node).
    soil_t = dc.daily_series(df, "soil_temp", node=snow_node)
    if soil_t.empty:
        soil_t = dc.daily_series(df, "soil_temp")
    soil_m = dc.daily_series(df, "soil_moisture", node=snow_node)
    if soil_m.empty:
        soil_m = dc.daily_series(df, "soil_moisture")

    events = detect(snow, soil_t, soil_m)

    lat, lon = locs.get(snow_node, (config.get("glees", {}).get("lat"),
                                    config.get("glees", {}).get("lon")))

    return {
        "layer": "snowmelt_recharge",
        "region": config.get("region"),
        "snow_node": snow_node,
        "melt_signal": signal_var,
        "melt_signal_unit": "mm (SWE)" if signal_var == "swe" else "m (snow depth)",
        "lat": lat,
        "lon": lon,
        "events": events,
        "has_snow_data": not snow.empty,
        "daily_series": {
            "melt_signal": {str(d.date()): round(float(v), 3) for d, v in snow.items()},
            "snow_depth_m": {str(d.date()): round(float(v), 3) for d, v in snow_depth.items()},
            "soil_temp_c": {str(d.date()): round(float(v), 2) for d, v in soil_t.items()},
            "soil_moisture": {str(d.date()): round(float(v), 4) for d, v in soil_m.items()},
        },
    }


def main():
    ap = argparse.ArgumentParser(description="Build snowmelt recharge timing layer")
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
    ev = result["events"]
    print(f"snowmelt_recharge: peak={ev['peak_snow_date']} "
          f"snow_free={ev['snow_free_date']} recharge_lag={ev['recharge_lag_days']}d")


if __name__ == "__main__":
    main()
