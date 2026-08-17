# Drought Workflow — Specification

**Status:** v1.2 · **Last updated:** 2026-06-22 · **Owner:** kthare10@renci.org

This document specifies the behaviour, interfaces, and data contracts of the
Drought workflow. The README is the user guide; this is the authoritative
description of *what each component must do*.

> **v1.1 note.** Updated to reflect the GLEES/Sage data-pipeline diagram shared by
> the Sage team (see §3). Key consequences: (a) the live operational path for
> GLEES data is the **Sage data API** fed by a **Thor Blade** edge node at GLEES —
> AmeriFlux US-GLE is the historical/validation archive, not the only source; and
> (b) the system is explicitly **two-tier edge-to-cloud**, which the workflow
> mirrors.
>
> **v1.2 note (no-blade strategy).** The Thor Blade is not yet accessible, so the
> workflow does **not** depend on it. The live source for snow/SWE/precip/air-temp
> is **SNOTEL Brooklyn Lake #367**, co-located with GLEES (~1 km), via the public
> NRCS AWDB REST API; AmeriFlux US-GLE supplies flux/soil/radiation. A new
> **`harmonize`** stage merges all sources into one analysis-ready dataset that
> every layer consumes, so the Sage/blade fetcher is just a dormant adapter that
> activates when a node VSN is configured. Implemented: SNOTEL fetch + harmonize
> (§7.3, §7.4); the layers now read the harmonized observations.

---

## 1. Purpose & Scope

Transform observations from the **GLEES forest instrument cluster** (delivered via
the **Sage** edge platform) plus supporting sources into five decision-support
layers for a subalpine forest watershed:

| # | Layer | Question answered |
|---|-------|-------------------|
| 1 | Soil moisture map | How wet is the root zone, where, right now? |
| 2 | Forest water stress | Is the canopy under water stress? |
| 3 | Snowmelt recharge timing | When did snowmelt recharge the soil? |
| 4 | Drought (early) warning | What is the drought severity / warning level? |
| 5 | Wildfire risk | What is the fire-weather risk, adjusted for fuel dryness? |

Plus a **decision-support dashboard** aggregating all five.

**Implemented today (v1.2):** multi-source fetch (SNOTEL #367, AmeriFlux US-GLE,
dormant Sage adapter), **harmonization**, normalisation, the five layers, the
dashboard, and a single-tier Pegasus DAG (cloud/local).

**Planned (v2+ — see §8):** Sage-native GLEES ingestion (when the blade is
available), QA/QC + sensor-health flags, anomaly detection, camera/thermal AI
inference, model training + forecasting, remote-sensing fusion, and an
edge-to-cloud (Thor Blade) deployment split.

---

## 2. Reference Domain

Snowy Range, Medicine Bow National Forest, WY. Anchor site GLEES Brooklyn Tower
(US-GLE): 41.3665 °N, −106.2399 °W, 3197 m, evergreen needleleaf (spruce-fir).
Domain is re-targetable entirely through `region_config.json`.

---

## 3. System Context — Sage / GLEES Data Architecture

The Sage team's pipeline has five tiers. This workflow consumes its outputs and
implements the "User Platforms → Applications" tiers; it can optionally run jobs
on the edge tier (Thor Blade) in the v2 edge-to-cloud build.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ TIER 1  GLEES Forest Site Sensors                                         │
│   soil moisture | snow | met station | flux/ET | radiation | cameras/      │
│   thermal.  Raw data over hardwires, LoRaWAN, etc.                         │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 2  Local sensor network / dataloggers                                │
│   Campbell loggers, LoRaWAN gateway. Sampling freq + sensor labels set here│
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 3  Thor Blade @ GLEES   (EDGE compute)                               │
│   edge acquisition + QA/QC + AI inference + local storage                  │
│   Edge products: sensor health flags · anomaly detection ·                 │
│   drought/snowmelt/plant-water-stress signals · wildfire moisture conds.   │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 4  Sage website                                                      │
│   data upload | metadata | node monitoring | model containers | access     │
│   → exposed via the Sage data API: https://data.sagecontinuum.org         │
├─────────────────────────────────────────────────────────────────────────┤
│ TIER 5  User Platforms        ◄── THIS WORKFLOW                           │
│   data harmonization | model training | forecasting | remote-sensing       │
│   integration                                                              │
├─────────────────────────────────────────────────────────────────────────┤
│         Applications: the 5 layers + decision-support dashboards           │
└─────────────────────────────────────────────────────────────────────────┘
```

**Implications for this workflow:**

- **Primary live source = Sage data API** (Tier 4). The GLEES Thor Blade node
  publishes both raw/harmonized sensor streams *and* edge products. Our
  `fetch_sage_data.py` is therefore the path to GLEES once the node's VSN is
  known — not a separate generic-Sage path. The richer sensor suite (snow,
  radiation, flux/ET, camera/thermal) must be representable in our variable
  vocabulary and config.
- **Camera/thermal** are file products (Sage `upload` records / object store),
  not scalar streams — they need an image-ingestion + inference path (§8.5).
- **Edge products are reusable.** Where the Thor Blade already emits anomaly
  flags, health flags, and water-stress signals, the workflow should *consume*
  them rather than recompute — and fall back to its own computation when absent.
- **AmeriFlux US-GLE** stays as the historical archive for model training and as
  a cross-check on the live Sage streams.

---

## 4. Edge-to-Cloud Deployment Model (planned v2)

Mirrors the repo's `--enable-dpu` pattern (soilmoisture / orcasound). Jobs are
tagged for an **edge site** (Thor Blade ARM compute at GLEES) or a **cloud site**
(condor pool / FABRIC), with HTCondor routing the placement.

| Tier | Site | Jobs |
|------|------|------|
| Edge (Thor Blade) | `--edge-site` | acquisition shim, QA/QC, sensor-health flags, anomaly detection, camera/thermal inference, quick water-stress/snowmelt signals |
| Cloud (User Platform) | `--cloud-site` | harmonization, model training, forecasting, remote-sensing fusion, the 5 layers, dashboard |

Rationale: keep I/O-heavy acquisition and latency-sensitive QA/inference near the
sensors; push compute-heavy training/forecasting/mapping to the cloud. v1 runs
everything on one site; v2 adds the split behind a flag (no logic change to the
layer scripts, only transformation site assignment).

---

## 5. Architecture & DAG

### v1.2 (implemented)

```
fetch_sage   ─┐
fetch_glees  ─┼─> harmonize ─┬─> soil_moisture_map ──┐
fetch_snotel ─┘              ├─> forest_water_stress ┼─> wildfire_risk ──┐
                             │        └──────────────┴─> drought_warning ┼─> dashboard
                             └─> snowmelt_recharge ─────────────────────-┘
```

All three fetch jobs feed `harmonize`, which emits the single `observations.csv`
that every layer consumes (plus a `harmonization_report.json`).

### v2 (planned — full pipeline)

```
[EDGE]  fetch_sage ─> qa_qc ─> sensor_health ─┐
                          └─> anomaly_detect ──┤
        camera/thermal ─> image_inference ─────┤
[CLOUD] fetch_glees ─> harmonize ◄─────────────┘
        remote_sensing_fetch ─> harmonize
                                   │
            ┌──────────────────────┼───────────────────────┐
            ▼                      ▼                       ▼
       soil_moisture_map   forest_water_stress      snowmelt_recharge
            │  └──────────┬──────────┘                     │
            │             ▼                                 │
            │      train_model ─> forecast                 │
            └──────────► drought_warning, wildfire_risk ◄──┘
                                   │
                                   ▼
                              dashboard
```

**Dependencies (must hold):**

- `harmonize` depends on all fetch jobs (`fetch_sage`, `fetch_glees`,
  `fetch_snotel`).
- `soil_moisture_map`, `forest_water_stress`, `snowmelt_recharge` depend on
  `harmonize`.
- `drought_warning` and `wildfire_risk` additionally depend on
  `soil_moisture_map` **and** `forest_water_stress`.
- `dashboard` depends on all five layer jobs.

**Execution model:** Pegasus WMS; sites `local` (storage) + a condor pool
(execution, v2 adds an edge site); jobs run inside the `kthare10/drought`
container. All files staged via the catalogs; no job assumes shared state beyond
its declared inputs.

---

## 6. Canonical Data Contract

Every fetcher MUST emit a **long-format CSV** with exactly these columns;
`harmonize` concatenates them, enforces one canonical unit per variable, drops
exact duplicates, and emits the single `observations.csv` all layers read:

| column | type | notes |
|--------|------|-------|
| `timestamp` | ISO-8601 UTC | parseable by `pandas.to_datetime(..., utc=True)` |
| `source` | string | `sage` \| `glees` \| `snotel` |
| `node` | string | Sage VSN, `US-GLE`, or `SNOTEL:367` |
| `lat` | float \| empty | required for map placement; may be empty |
| `lon` | float \| empty | "" |
| `variable` | enum | see vocabulary below |
| `value` | float | normalised units |
| `unit` | string | canonical unit string |

**Normalised variable vocabulary & units** (`drought_common.VARIABLES`):

| variable | unit | normalisation rule |
|----------|------|--------------------|
| `soil_moisture` | m³/m³ (0–1) | percent → fraction (÷100 when >1.5) |
| `soil_temp` | °C | — |
| `air_temp` | °C | — |
| `rel_humidity` | percent | clipped 0–100 |
| `pressure` | Pa | kPa → Pa (×1000) |
| `wind_speed` | m/s | — |
| `precip` | mm | — |
| `le` | W/m² | latent heat flux |
| `h` | W/m² | sensible heat flux |
| `netrad` | W/m² | net radiation |
| `snow_depth` | m | cm → m (×0.01); SNOTEL in → m (×0.0254) |
| `swe` | mm | snow water equivalent; SNOTEL in → mm (×25.4) |
| `vpd` | kPa | hPa → kPa (×0.1) |

`swe`, `et`, `sw_in`/`sw_out`/`lw_in`/`lw_out`, and `surface_temp` are in the
canonical vocabulary (`drought_common.VARIABLES`); `swe` is populated today by
SNOTEL. **Still planned (camera-derived, §8.5):** `snow_cover_fraction` (0–1),
`canopy_greenness` (GCC index) — produced by the inference stage, not fetchers.

**SNOTEL specifics:** soil moisture/temperature are reported per depth; the
fetcher emits one row per depth and downstream daily aggregation averages them.
TOBS/TAVG both map to `air_temp`; `harmonize` dedupes the same-day collision.
Brooklyn Lake #367 is a basic snow-pillow site (no soil-moisture sensors), so
`soil_moisture` comes from GLEES — the motivating case for multi-source merge.

**Missing-data rule:** AmeriFlux `-9999` → NaN and dropped; empty observation
files are valid and MUST NOT crash any downstream layer. Where edge products
(health/anomaly flags) are present they carry into the contract as additional
`variable` rows (e.g. `health_flag`, `anomaly_score`).

---

## 7. Component Specifications — Implemented (v1.2)

### 7.1 `fetch_sage_data.py`

- **In:** `region_config.json`, optional `--start-date/--end-date`.
- **Out:** observations CSV (`source=sage`).
- **Behaviour:** for each `(measurement, vsn)` POST to
  `https://data.sagecontinuum.org/api/v1/query`; parse NDJSON; attach node
  lat/lon from config; drop nodes outside `bbox` when coordinates are known.
- **Guard:** if `sage.nodes` is empty, MUST skip all queries (no unbounded
  network-wide pull) and emit an empty, valid CSV with a warning.
- **Failure:** a failed HTTP query logs a warning and yields zero rows; it MUST
  NOT abort the job.
- **v2 extension:** add the GLEES Thor Blade VSN and its full measurement set
  (snow/radiation/flux/camera) to config; this becomes the primary GLEES path.

### 7.2 `fetch_glees_data.py`

- **In:** `region_config.json`; credentials via `AMERIFLUX_USER_ID` /
  `AMERIFLUX_USER_EMAIL`, or `--base-zip` / `--base-csv`.
- **Out:** observations CSV (`source=glees`).
- **Behaviour:** POST `BASE-BADM` request to
  `https://amfcdn.lbl.gov/api/v1/data_download`, download the returned zip,
  parse the `_BASE_` CSV (skip `#` header), average replicate sensor columns
  (`TOKEN_x_y_z`), map via `VARMAP`, filter to the date window.
- **Role (v1.1):** historical/validation archive; the live path is §7.1.
- **Failure:** missing credentials and no local file → exit code 2 with a clear
  message.

### 7.3 `fetch_snotel_data.py`

- **In:** `region_config.json` (`snotel.station_triplet`, `node`, `lat/lon`,
  `elements`, `duration`), optional `--start-date/--end-date`.
- **Out:** observations CSV (`source=snotel`).
- **Behaviour:** GET the NRCS AWDB REST API
  `https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data`; map element codes
  (`WTEQ→swe`, `SNWD→snow_depth`, `PRCP→precip`, `TOBS/TAVG→air_temp`,
  `SMS→soil_moisture`, `STO→soil_temp`, `RHUM→rel_humidity`, `WSPD→wind_speed`),
  converting imperial → canonical units. If only accumulated `PREC` is returned,
  derive the increment by diff.
- **Guard:** no `snotel.station_triplet` → empty, valid CSV with a warning.
- **Failure:** a failed query logs a warning and yields zero rows (non-fatal).
- **Role (v1.2):** primary live source for snow/SWE/precip/air-temp; stands in
  for the Thor Blade until that node is accessible.

### 7.4 `harmonize.py`

- **In:** `--inputs` (the sage/glees/snotel observation CSVs), `--output`,
  optional `--report`.
- **Out:** single `observations.csv` (the only input to all layers) +
  `harmonization_report.json`.
- **Behaviour:** concatenate; enforce one canonical unit per variable (warn on
  mismatch / out-of-vocabulary); drop exact duplicates on
  `(source, node, variable, timestamp)`; sort; emit provenance + coverage report.
- **Invariant:** sources stay distinct nodes (GLEES tower vs co-located SNOTEL)
  so complementary instruments are preserved, not collapsed.

### 7.5 `soil_moisture_map.py` (Layer 1)

- Per node: current SWC, period mean/min, anomaly, dryness class
  (`very_dry≤0.10 < dry≤0.20 < moderate≤0.30 < moist≤0.40 < saturated`).
- IDW grid produced **iff ≥3 located points**; else `grid=null`.

### 7.6 `forest_water_stress.py` (Layer 2)

- **FWSI** = `0.40·(1−EF) + 0.35·norm(VPD) + 0.25·(1−norm(SM))`, clipped 0–1.
  `EF = LE/(LE+H)` over daytime hours (`netrad>50`). At nodes lacking flux data,
  drop the EF term and re-weight the remainder to sum to 1.
- Classes: `none≤0.2 < low≤0.4 < moderate≤0.6 < high≤0.8 < severe`.

### 7.7 `snowmelt_recharge.py` (Layer 3)

- Melt signal = **SWE** (best recharge driver) from whichever node reports it
  (typically SNOTEL #367), else snow depth. Soil temp/moisture from the same node
  if present, else region-wide.
- Daily series smoothed (3-day centred mean). Thresholds are **peak-relative** so
  the same logic works for SWE (mm) or snow depth (m): melt onset = ≥3 days losing
  >1% of peak/day; snow-free = first day ≤5% of peak; soil thaw = `soil_temp>0`;
  recharge onset = ≥3 days >0.005 SWC/day rise; plus recharge lag and magnitude.
  Each field nullable when its driver series is absent.

### 7.8 `drought_warning.py` (Layer 4)

- **DSI** = `0.35·(1−SM_percentile) + 0.25·FWSI + 0.20·precip_deficit +
  0.20·norm(VPD)`, daily, clipped 0–1; missing term → neutral default.
- Category: `None≤0.20 < D0≤0.40 < D1≤0.60 < D2≤0.80 < D3≤0.90 < D4`;
  level None→normal, D0→watch, D1/D2→warning, D3/D4→emergency.

### 7.9 `wildfire_risk.py` (Layer 5)

- **Fosberg FFWI** from `air_temp`, `rel_humidity`, `wind_speed` (peak hour/day;
  missing wind → region median, flagged), × `(0.6 + 0.4·fuel_dryness)` where
  `fuel_dryness = 0.5·FWSI_region + 0.5·clip(1 − SM_region/0.40, 0, 1)`.
- Classes: `low≤20 < moderate≤40 < high≤60 < very_high≤75 < extreme`.

### 7.10 `visualize_dashboard.py`

- Reads the five layer JSONs + config → 2×3 panel PNG (3 maps, snowmelt
  timeline, DSI trend, text summary). Degrades gracefully on missing layers.

---

## 8. Component Specifications — Planned (v2/v3)

Each is additive and respects the §6 data contract. Status is a proposal until
prioritised.

### 8.1 `qa_qc.py` (edge)
Range/spike/flatline/stuck-sensor checks per stream; output the cleaned
observations CSV plus a per-(node,variable) QC report. Consume the Thor Blade's
own QA/QC flags when present, else apply default rules. **Feasibility: high** —
pure pandas; no new dependency.

### 8.2 `sensor_health.py` (edge)
Roll QC + gap statistics into a per-sensor health flag (ok/degraded/offline) and
a node-level health summary, mirroring the Thor Blade "sensor health flags"
product. **Feasibility: high.**

### 8.3 `anomaly_detection.py` (edge)
Per-stream anomaly score (robust z-score / seasonal-residual / optional
IsolationForest via scikit-learn) → `anomaly_score` contract rows + flagged
events. Prefer the edge product when published. **Feasibility: high**
(scikit-learn already in scope for v2).

### 8.4 `harmonize.py` (cloud) — ✅ IMPLEMENTED (see §7.4)
Merges all source CSVs onto the common schema/units with provenance and is the
single input to every layer. **Done in v1.2.** Remaining v2 work: time-grid
resampling and reconciling *duplicate variables across co-located nodes* (e.g.
live SWC vs AmeriFlux SWC) rather than keeping them as separate nodes.

### 8.5 `image_inference.py` (edge)
Ingest Sage camera/thermal `upload` records; run inference to derive
`snow_cover_fraction` (phenocam snow classification), `canopy_greenness` (GCC),
and `surface_temp` (thermal) as contract rows. **Feasibility: medium** — needs an
image fetch path + a model container (start with classical CV: GCC + snow
thresholding; add a CNN later). Pairs with the repo's crophealth/orcasound
image-handling patterns.

### 8.6 `train_model.py` + `forecast.py` (cloud)
Train per-target models (soil moisture, FWSI, DSI) on AmeriFlux history + Sage
streams; produce N-day forecasts feeding a **drought *early* warning** (the
diagram's wording). Reuse the soilmoisture-workflow LSTM scaffold.
**Feasibility: medium** — needs torch in the container and a training data
window; start with gradient-boosting/linear baselines.

### 8.7 `remote_sensing_fetch.py` (cloud)
Pull SMAP (soil moisture), MODIS/VIIRS (snow cover, LST, NDVI), and gridMET into
the bbox; feed `harmonize` so the maps become true gridded rasters rather than
point-IDW. **Feasibility: medium** — public APIs (NSIDC/AppEEARS/USGS); auth and
raster handling add weight (xarray/rasterio).

### 8.8 Edge-to-cloud generator flags
`--enable-edge --edge-site <s> --cloud-site <s>` on `workflow_generator.py`:
assign §4 transformations to sites, no layer-logic change. **Feasibility: high** —
copy the soilmoisture `--enable-dpu` mechanics.

---

## 9. Map Output Schema (layers 1, 2, 5)

```json
{
  "layer": "<name>", "units": "...", "region": "...",
  "n_locations": <int>, "classes": ["..."],
  "points": [{"node","lat","lon","<value_key>","class", ...}],
  "grid": {"lats": [...], "lons": [...], "values": [[...]]} | null,
  "daily_series": {"<node>": {"YYYY-MM-DD": <float>}}
}
```

---

## 10. Configuration Schema (`region_config.json`)

| key | required | meaning |
|-----|----------|---------|
| `region` | yes | label used in outputs |
| `bbox.{min,max}_{lat,lon}` | yes | map extent + Sage spatial filter |
| `date_range.{start,end}` | yes | default fetch window (YYYY-MM-DD) |
| `glees.{site_id,lat,lon,...}` | yes | US-GLE anchor (AmeriFlux historical) |
| `snotel.{station_triplet,node,lat,lon,elements,duration}` | yes | NRCS SNOTEL station (primary live snow/SWE/precip source) |
| `sage.nodes[]` | no | `{vsn,lat,lon}`; includes the GLEES Thor Blade once known |
| `sage.measurements` | yes | measurement-name → variable mapping |

---

## 11. Non-Functional Requirements

- **Determinism:** identical inputs → identical outputs (no wall-clock in logic).
- **Resilience:** empty/partial inputs never crash a layer; consume edge products
  when present, recompute when absent.
- **Portability:** pure-Python + pandas/numpy/scipy/matplotlib (v1), runs in the
  container; no host services. v2 image/ML/raster deps are container-scoped.
- **Reproducibility:** pinned `requirements.txt` + a versioned Apptainer image
  (`Apptainer/Drought_Container.def` built to a `.sif` that Pegasus stages;
  new filename per dependency change rather than mutating one in place).
- **Privacy:** credentials only via environment, never written into the DAG.

---

## 12. Validation

- Every script passes `python -m py_compile`.
- End-to-end smoke test: **real SNOTEL #367** + a synthetic GLEES BASE season,
  merged through `harmonize`, exercise all five layers + dashboard. Confirms the
  multi-source path (SNOTEL SWE drives snowmelt; GLEES drives soil/flux). See
  `example_usage.sh`.

---

## 13. Roadmap

| Phase | Deliverable | From diagram tier |
|-------|-------------|-------------------|
| **v1 (done)** | 5 layers + dashboard, single-tier DAG, AmeriFlux + Sage fetch | Tier 5 / Applications |
| **v1.2 (done)** | SNOTEL #367 live fetch + `harmonize`; layers source-agnostic (no-blade strategy) | Tiers 1–4 (public proxy) |
| **v2a** | Sage-native GLEES node ingestion (snow/radiation/flux when blade is up), `qa_qc`, `sensor_health`, `anomaly_detection` | Tiers 3–4 products |
| **v2b** | Edge-to-cloud split (§4, §8.8) on Thor Blade vs cloud | Tier 3 edge compute |
| **v2c** | `image_inference` for camera/thermal → snow cover, greenness, surface temp | Tier 1 cameras |
| **v3** | `train_model` + `forecast` (drought *early* warning), `remote_sensing_fetch` (gridded maps) | Tier 5 model training / forecasting / RS integration |
