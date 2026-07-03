#!/usr/bin/env python3
"""Final stage - render the five drought layers into one dashboard PNG.

Reads the layer JSONs and the region config and produces a 2x3 panel figure:
soil-moisture map, forest-water-stress map, wildfire-risk map, snowmelt timeline,
drought DSI trend, and a text summary.
"""

import argparse
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402


def load(path):
    if path and os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return None


def _series(d):
    if not d:
        return pd.Series(dtype=float)
    s = pd.Series(d)
    s.index = pd.to_datetime(s.index)
    return s.sort_index()


def map_panel(ax, layer, bbox, cmap, title, value_key):
    ax.set_title(title, fontsize=11, fontweight="bold")
    if not layer:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        ax.set_xticks([]); ax.set_yticks([]); return

    grid = layer.get("grid")
    if grid and grid.get("values"):
        lats, lons = np.array(grid["lats"]), np.array(grid["lons"])
        vals = np.array(grid["values"])
        im = ax.contourf(lons, lats, vals, levels=12, cmap=cmap, alpha=0.85)
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

    pts = [p for p in layer.get("points", []) if p.get("lat") and p.get("lon")]
    if pts:
        xs = [p["lon"] for p in pts]
        ys = [p["lat"] for p in pts]
        cs = [p.get(value_key) for p in pts]
        sc = ax.scatter(xs, ys, c=cs, cmap=cmap, s=140, edgecolor="k",
                        linewidth=0.8, zorder=3)
        if not grid:
            plt.colorbar(sc, ax=ax, fraction=0.046, pad=0.04)
        for p in pts:
            ax.annotate(p["node"], (p["lon"], p["lat"]), fontsize=7,
                        xytext=(4, 4), textcoords="offset points")
    else:
        ax.text(0.5, 0.5, "no located points", ha="center", va="center",
                transform=ax.transAxes, fontsize=9)

    if bbox:
        ax.set_xlim(bbox["min_lon"], bbox["max_lon"])
        ax.set_ylim(bbox["min_lat"], bbox["max_lat"])
    ax.set_xlabel("lon"); ax.set_ylabel("lat")


def snowmelt_panel(ax, layer):
    ax.set_title("Snowmelt & Recharge Timing", fontsize=11, fontweight="bold")
    if not layer:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        return
    ds = layer.get("daily_series", {})
    snow = _series(ds.get("snow_depth_m"))
    sm = _series(ds.get("soil_moisture"))
    if not snow.empty:
        ax.plot(snow.index, snow.values, color="steelblue", label="snow depth (m)")
        ax.set_ylabel("snow depth (m)", color="steelblue")
    ax.tick_params(axis="x", rotation=45)
    if not sm.empty:
        ax2 = ax.twinx()
        ax2.plot(sm.index, sm.values, color="saddlebrown", label="soil moisture")
        ax2.set_ylabel("soil moisture", color="saddlebrown")

    ev = layer.get("events", {})
    colors = {"peak_snow_date": "navy", "snow_free_date": "green",
              "recharge_onset_date": "orange"}
    for key, col in colors.items():
        d = ev.get(key)
        if d:
            ax.axvline(pd.to_datetime(d), color=col, ls="--", lw=1,
                       label=key.replace("_date", ""))
    ax.legend(fontsize=7, loc="upper right")


def drought_panel(ax, layer):
    ax.set_title("Drought Severity Index (trend)", fontsize=11, fontweight="bold")
    if not layer:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", transform=ax.transAxes)
        return
    dsi = _series(layer.get("daily_dsi"))
    bands = [(0, 0.2, "#ffffff", "None"), (0.2, 0.4, "#ffff00", "D0"),
             (0.4, 0.6, "#fcd37f", "D1"), (0.6, 0.8, "#ffaa00", "D2"),
             (0.8, 0.9, "#e60000", "D3"), (0.9, 1.0, "#730000", "D4")]
    for lo, hi, col, _ in bands:
        ax.axhspan(lo, hi, color=col, alpha=0.35)
    if not dsi.empty:
        ax.plot(dsi.index, dsi.values, color="black", lw=2)
    ax.set_ylim(0, 1); ax.set_ylabel("DSI")
    ax.tick_params(axis="x", rotation=45)


def summary_panel(ax, sm, fws, snow, drought, fire, config):
    ax.axis("off")
    lines = [f"DROUGHT DASHBOARD - {config.get('region', '')}", ""]
    if sm:
        lines.append(f"Soil moisture (region): {sm.get('region_mean_current')} m3/m3 "
                     f"across {sm.get('n_locations')} sites")
    if fws:
        lines.append(f"Forest water stress: FWSI {fws.get('region_mean_fwsi')}")
    if snow:
        ev = snow.get("events", {})
        lines.append(f"Snow: peak {ev.get('peak_snow_date')}, "
                     f"snow-free {ev.get('snow_free_date')}, "
                     f"recharge lag {ev.get('recharge_lag_days')} d")
    if drought:
        lines.append(f"Drought: {drought.get('category')} "
                     f"(DSI {drought.get('current_dsi')}, "
                     f"{drought.get('warning_level')})")
    if fire:
        lines.append(f"Wildfire risk: {drought_fmt(fire)}")
    lines.append("")
    lines.append("Sources: SNOTEL #367 (snow/SWE/precip) + AmeriFlux US-GLE")
    lines.append("         (flux/soil) + Sage edge sensors (when configured)")
    ax.text(0.02, 0.98, "\n".join(lines), va="top", ha="left", fontsize=10,
            family="monospace", transform=ax.transAxes)


def drought_fmt(fire):
    return f"{fire.get('region_mean_risk')} ({fire.get('region_class')})"


def main():
    ap = argparse.ArgumentParser(description="Render drought dashboard")
    ap.add_argument("--soil-moisture")
    ap.add_argument("--forest-stress")
    ap.add_argument("--snowmelt")
    ap.add_argument("--drought-warning")
    ap.add_argument("--wildfire")
    ap.add_argument("--config", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    with open(args.config) as fh:
        config = json.load(fh)
    bbox = config.get("bbox", {})

    sm = load(args.soil_moisture)
    fws = load(args.forest_stress)
    snow = load(args.snowmelt)
    drought = load(args.drought_warning)
    fire = load(args.wildfire)

    fig, axes = plt.subplots(2, 3, figsize=(18, 11))
    fig.suptitle(f"Drought / Forest-Water / Wildfire Dashboard - {config.get('region','')}",
                 fontsize=15, fontweight="bold")

    map_panel(axes[0, 0], sm, bbox, "YlGnBu", "Soil Moisture (m3/m3)", "current")
    map_panel(axes[0, 1], fws, bbox, "OrRd", "Forest Water Stress (FWSI)", "fwsi")
    map_panel(axes[0, 2], fire, bbox, "YlOrRd", "Wildfire Risk (FFWI)", "risk_index")
    snowmelt_panel(axes[1, 0], snow)
    drought_panel(axes[1, 1], drought)
    summary_panel(axes[1, 2], sm, fws, snow, drought, fire, config)

    fig.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(args.output, dpi=130)
    print(f"dashboard -> {args.output}")


if __name__ == "__main__":
    main()
