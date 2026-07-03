#!/usr/bin/env python3
"""Fetch GLEES (AmeriFlux US-GLE) flux-tower data and normalise it.

AmeriFlux data are *not* served by an anonymous REST API: downloading requires a
free registered account and acceptance of the CC-BY-4.0 data policy. We use the
official AmeriFlux download service (the same endpoint the `amerifluxr` package
drives):

    POST https://amfcdn.lbl.gov/api/v1/data_download

Credentials are read from the environment so they never live in the workflow:

    AMERIFLUX_USER_ID     your AmeriFlux username
    AMERIFLUX_USER_EMAIL  the email registered with AmeriFlux

If credentials are absent you may instead point --base-csv / --base-zip at a
BASE file you downloaded by hand from https://ameriflux.lbl.gov. Either way we
parse the half-hourly BASE product, map the FP-standard columns onto the common
variable vocabulary, and emit the same long-format CSV as the Sage fetcher:

    timestamp, source, node, lat, lon, variable, value, unit
"""

import argparse
import io
import json
import logging
import os
import sys
import zipfile

import numpy as np
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("fetch_glees")

DOWNLOAD_URL = "https://amfcdn.lbl.gov/api/v1/data_download"
MISSING = -9999.0

COLUMNS = ["timestamp", "source", "node", "lat", "lon", "variable", "value", "unit"]

# FP-standard BASE token -> (normalised variable, unit, scale, offset)
# AmeriFlux columns carry position qualifiers (e.g. SWC_1_1_1); we match on the
# leading token and average across replicate sensors.
VARMAP = {
    "TA": ("air_temp", "degC", 1.0, 0.0),
    "RH": ("rel_humidity", "percent", 1.0, 0.0),
    "WS": ("wind_speed", "m/s", 1.0, 0.0),
    "PA": ("pressure", "Pa", 1000.0, 0.0),  # kPa -> Pa
    "P": ("precip", "mm", 1.0, 0.0),
    "LE": ("le", "W/m2", 1.0, 0.0),
    "H": ("h", "W/m2", 1.0, 0.0),
    "NETRAD": ("netrad", "W/m2", 1.0, 0.0),
    "SWC": ("soil_moisture", "m3/m3", 0.01, 0.0),  # % volumetric -> fraction
    "TS": ("soil_temp", "degC", 1.0, 0.0),
    "D_SNOW": ("snow_depth", "m", 0.01, 0.0),  # cm -> m
    "VPD": ("vpd", "kPa", 0.1, 0.0),  # hPa -> kPa
}


def download_base_zip(site_id: str) -> bytes:
    """Request a BASE download from AmeriFlux and return the zip bytes."""
    user_id = os.environ.get("AMERIFLUX_USER_ID")
    user_email = os.environ.get("AMERIFLUX_USER_EMAIL")
    if not (user_id and user_email):
        raise RuntimeError(
            "AmeriFlux credentials missing. Set AMERIFLUX_USER_ID and "
            "AMERIFLUX_USER_EMAIL (free registration at https://ameriflux.lbl.gov/"
            "data/download-data/), or pass --base-csv / --base-zip with a file you "
            "downloaded manually."
        )

    payload = {
        "user_id": user_id,
        "user_email": user_email,
        "data_product": "BASE-BADM",
        "data_policy": "CCBY4.0",
        "site_ids": [site_id],
        "intended_use": "research",
        "description": "Automated drought/snowmelt/wildfire monitoring workflow.",
        "is_test": False,
    }
    logger.info("Requesting AmeriFlux BASE download for %s", site_id)
    resp = requests.post(DOWNLOAD_URL, json=payload, timeout=180)
    resp.raise_for_status()
    info = resp.json()

    # The service returns data_urls as a list of {site_id, url, ...} dicts (the
    # url carries a query string, e.g. ...AMF_US-GLE_BASE-BADM_9-5.zip?=user).
    # Tolerate both that shape and a plain list/dict of URL strings.
    raw = info.get("data_urls") or info.get("manifest") or []
    if isinstance(raw, dict):
        raw = list(raw.values())
    urls = []
    for u in raw:
        if isinstance(u, dict):
            u = u.get("url") or u.get("download_url") or ""
        if isinstance(u, str) and ".zip" in u:
            urls.append(u)
    if not urls:
        raise RuntimeError(f"AmeriFlux returned no download URLs: {info}")

    logger.info("Downloading %s", urls[0])
    data = requests.get(urls[0], timeout=300)
    data.raise_for_status()
    return data.content


def read_base_csv_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    """Extract the BASE CSV from a downloaded AmeriFlux zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        base_names = [n for n in zf.namelist() if "_BASE_" in n and n.endswith(".csv")]
        if not base_names:
            raise ValueError("No BASE CSV found inside AmeriFlux zip")
        with zf.open(base_names[0]) as fh:
            return _read_base_stream(fh)


def _read_base_stream(fh) -> pd.DataFrame:
    """Read a BASE CSV stream, skipping the leading '#' comment header."""
    return pd.read_csv(fh, comment="#", na_values=[MISSING, str(MISSING)])


def normalise(df: pd.DataFrame, site_id: str, lat: float, lon: float) -> pd.DataFrame:
    """Map BASE columns onto the common long-format schema."""
    if "TIMESTAMP_START" not in df.columns:
        raise ValueError("BASE file missing TIMESTAMP_START column")
    ts = pd.to_datetime(
        df["TIMESTAMP_START"].astype("Int64").astype(str),
        format="%Y%m%d%H%M",
        errors="coerce",
        utc=True,
    )

    rows = []
    for token, (variable, unit, scale, offset) in VARMAP.items():
        # Match exact token or token with position qualifiers (TOKEN_x_y_z),
        # but avoid false hits like 'P' matching 'PA' or 'TA' matching 'TAU'.
        cols = [
            c
            for c in df.columns
            if c == token or c.startswith(token + "_")
        ]
        if not cols:
            continue
        vals = df[cols].replace(MISSING, np.nan)
        mean = vals.mean(axis=1, skipna=True) * scale + offset
        sub = pd.DataFrame(
            {
                "timestamp": ts,
                "source": "glees",
                "node": site_id,
                "lat": lat,
                "lon": lon,
                "variable": variable,
                "value": mean,
                "unit": unit,
            }
        )
        rows.append(sub.dropna(subset=["value", "timestamp"]))

    if not rows:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(rows, ignore_index=True)[COLUMNS]


def main():
    ap = argparse.ArgumentParser(description="Fetch GLEES (AmeriFlux US-GLE) data")
    ap.add_argument("--config", required=True, help="region_config.json")
    ap.add_argument("--start-date", help="YYYY-MM-DD filter (overrides config)")
    ap.add_argument("--end-date", help="YYYY-MM-DD filter (overrides config)")
    ap.add_argument("--base-zip", help="Pre-downloaded AmeriFlux BASE zip")
    ap.add_argument("--base-csv", help="Pre-extracted AmeriFlux BASE CSV")
    ap.add_argument("--output", required=True, help="Output observations CSV")
    args = ap.parse_args()

    with open(args.config) as fh:
        config = json.load(fh)
    glees = config.get("glees", {})
    site_id = glees.get("site_id", "US-GLE")
    lat, lon = glees.get("lat"), glees.get("lon")

    cols = ["timestamp", "source", "node", "lat", "lon", "variable", "value", "unit"]
    try:
        if args.base_csv:
            with open(args.base_csv, "rb") as fh:
                raw = _read_base_stream(fh)
        else:
            if args.base_zip:
                with open(args.base_zip, "rb") as fh:
                    zip_bytes = fh.read()
            else:
                zip_bytes = download_base_zip(site_id)
            raw = read_base_csv_from_zip(zip_bytes)
        df = normalise(raw, site_id, lat, lon)
    except Exception as exc:
        # Best-effort source: degrade gracefully (empty file, exit 0) so a GLEES
        # download/parse problem doesn't block the multi-source workflow.
        # harmonize fails only if every source ends up empty. Log loudly.
        logger.error("GLEES unavailable; continuing with an empty file "
                     "(harmonize fails only if every source is empty): %s", exc)
        pd.DataFrame(columns=cols).to_csv(args.output, index=False)
        return

    dr = config.get("date_range", {})
    start = args.start_date or dr.get("start")
    end = args.end_date or dr.get("end")
    if start:
        df = df[df["timestamp"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        df = df[df["timestamp"] <= pd.Timestamp(end, tz="UTC")]

    df.to_csv(args.output, index=False)
    if df.empty:
        logger.warning("No GLEES observations in window %s..%s", start, end)
    else:
        logger.info(
            "Wrote %d GLEES observations (%d variables) -> %s",
            len(df),
            df["variable"].nunique(),
            args.output,
        )


if __name__ == "__main__":
    main()
