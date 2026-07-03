#!/usr/bin/env python3
"""Fetch NRCS SNOTEL data and normalise it onto the common schema.

SNOTEL (Brooklyn Lake #367) is co-located with the GLEES tower (~1 km) and is a
fully public, near-real-time source for snow water equivalent, snow depth,
precipitation, soil moisture/temperature (multiple depths) and air temperature.
It stands in for the GLEES Thor Blade edge node until that node is accessible.

Anonymous AWDB REST API:
    GET https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data
        ?stationTriplets=367:WY:SNTL&elements=WTEQ,SNWD,...&duration=DAILY
        &beginDate=YYYY-MM-DD&endDate=YYYY-MM-DD

SNOTEL reports imperial units; we convert to the canonical units and emit the
long-format CSV used by every layer:

    timestamp, source, node, lat, lon, variable, value, unit
"""

import argparse
import json
import logging
import sys
import time

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("fetch_snotel")

DATA_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"

COLUMNS = ["timestamp", "source", "node", "lat", "lon", "variable", "value", "unit"]

# SNOTEL element code -> (normalised variable, unit, converter).
IN_TO_MM = lambda v: v * 25.4
IN_TO_M = lambda v: v * 0.0254
F_TO_C = lambda v: (v - 32.0) * 5.0 / 9.0
PCT_TO_FRAC = lambda v: v / 100.0
MPH_TO_MS = lambda v: v * 0.44704

ELEMENT_MAP = {
    "WTEQ": ("swe", "mm", IN_TO_MM),
    "SNWD": ("snow_depth", "m", IN_TO_M),
    "PRCP": ("precip", "mm", IN_TO_MM),       # precipitation increment (preferred)
    "TOBS": ("air_temp", "degC", F_TO_C),
    "TAVG": ("air_temp", "degC", F_TO_C),
    "SMS": ("soil_moisture", "m3/m3", PCT_TO_FRAC),
    "STO": ("soil_temp", "degC", F_TO_C),
    "RHUM": ("rel_humidity", "percent", lambda v: v),
    "WSPD": ("wind_speed", "m/s", MPH_TO_MS),
}


def _query_awdb(params, timeout=90, retries=4, backoff=5):
    """GET the AWDB REST API, retrying transient failures.

    The public AWDB service occasionally times out or 5xx's under load. A single
    blip must not look like "no data", so we retry with linear backoff and raise
    on final failure — the caller turns that into a non-zero exit so Pegasus can
    retry the job instead of silently producing an empty observations file.
    """
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(DATA_URL, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last = exc
            logger.warning("SNOTEL query attempt %d/%d failed: %s",
                           attempt, retries, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"SNOTEL query failed after {retries} attempts: {last}")


def fetch(snotel: dict, start: str, end: str, timeout: int = 90) -> pd.DataFrame:
    """Query the AWDB REST API and normalise the response.

    Raises on a persistent API failure (vs. a successful-but-empty response,
    which returns an empty frame).
    """
    triplet = snotel["station_triplet"]
    node = snotel.get("node", f"SNOTEL:{triplet.split(':')[0]}")
    lat, lon = snotel.get("lat"), snotel.get("lon")
    elements = snotel.get("elements", list({v for v in ELEMENT_MAP}))

    params = {
        "stationTriplets": triplet,
        "elements": ",".join(elements),
        "duration": snotel.get("duration", "DAILY"),
        "beginDate": start,
        "endDate": end,
        "periodRef": "END",
        "returnFlags": "false",
    }
    payload = _query_awdb(params, timeout=timeout)

    # Response: [ {stationTriplet, data: [ {stationElement:{elementCode,...},
    #             values:[{date,value}, ...]} ]} ]
    if isinstance(payload, dict):
        payload = [payload]

    rows = []
    have_increment = False
    prec_accum = []  # fall back to diffing accumulated PREC if no PRCP increment
    for station in payload or []:
        for series in station.get("data", []):
            elem = series.get("stationElement", {}) or {}
            code = elem.get("elementCode")
            values = series.get("values", []) or []
            if code == "PRCP" and values:
                have_increment = True
            if code == "PREC":
                for v in values:
                    if v.get("value") is not None:
                        prec_accum.append((v.get("date"), float(v["value"])))
                continue
            mapping = ELEMENT_MAP.get(code)
            if not mapping:
                continue
            variable, unit, convert = mapping
            for v in values:
                val = v.get("value")
                if val is None:
                    continue
                rows.append({
                    "timestamp": v.get("date"),
                    "source": "snotel",
                    "node": node,
                    "lat": lat,
                    "lon": lon,
                    "variable": variable,
                    "value": convert(float(val)),
                    "unit": unit,
                })

    # If no increment precip was returned, derive it from accumulated PREC.
    if not have_increment and prec_accum:
        s = pd.Series(
            {d: v for d, v in prec_accum}
        ).sort_index()
        incr = s.diff().clip(lower=0).fillna(0.0)
        for d, v in incr.items():
            rows.append({
                "timestamp": d, "source": "snotel", "node": node,
                "lat": lat, "lon": lon, "variable": "precip",
                "value": IN_TO_MM(float(v)), "unit": "mm",
            })

    df = pd.DataFrame(rows, columns=COLUMNS)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.dropna(subset=["timestamp"])
    return df


def main():
    ap = argparse.ArgumentParser(description="Fetch NRCS SNOTEL data")
    ap.add_argument("--config", required=True, help="region_config.json")
    ap.add_argument("--start-date", help="YYYY-MM-DD (overrides config)")
    ap.add_argument("--end-date", help="YYYY-MM-DD (overrides config)")
    ap.add_argument("--output", required=True, help="Output observations CSV")
    args = ap.parse_args()

    with open(args.config) as fh:
        config = json.load(fh)
    snotel = config.get("snotel")
    if not snotel or not snotel.get("station_triplet"):
        logger.warning("No snotel.station_triplet configured; writing empty file.")
        pd.DataFrame(columns=COLUMNS).to_csv(args.output, index=False)
        return

    dr = config.get("date_range", {})
    start = args.start_date or dr.get("start")
    end = args.end_date or dr.get("end")
    if not start or not end:
        logger.error("start/end date required")
        sys.exit(1)

    try:
        df = fetch(snotel, start, end)
    except RuntimeError as exc:
        # Best-effort source in a multi-source merge: after retries, degrade
        # gracefully (empty file, exit 0) so a flaky API does not block the whole
        # workflow. harmonize merges whatever sources are available and fails only
        # if ALL of them are empty. Log loudly so the miss stays visible.
        logger.error("SNOTEL unavailable after retries; continuing with an empty "
                     "file (harmonize will fail only if every source is empty): %s",
                     exc)
        df = pd.DataFrame(columns=COLUMNS)

    df.to_csv(args.output, index=False)
    if df.empty:
        logger.warning("No SNOTEL observations for %s..%s", start, end)
    else:
        logger.info("Wrote %d SNOTEL observations (%d variables) -> %s",
                    len(df), df["variable"].nunique(), args.output)


if __name__ == "__main__":
    main()
