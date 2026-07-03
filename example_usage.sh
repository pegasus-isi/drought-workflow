#!/usr/bin/env bash
#
# Quick-start for the drought / forest-water / wildfire workflow.
#
# GLEES (AmeriFlux US-GLE) requires a free account to download data. Either:
#   export AMERIFLUX_USER_ID="your_username"
#   export AMERIFLUX_USER_EMAIL="you@example.org"
# ...or pass a BASE CSV you downloaded by hand via --glees-base-csv.
#
set -euo pipefail
cd "$(dirname "$0")"

CONFIG="region_config.json"

echo "==> 1. Generate the Pegasus DAG"
python workflow_generator.py --config "$CONFIG" -o workflow.yml
#  add: --glees-base-csv path/to/AMF_US-GLE_BASE_HH_*.csv   (skip credentialed download)
#  add: --sources snotel                                    (only fetch selected sources;
#                                                            SNOTEL-only needs no credentials)

echo "==> 2. Plan & submit"
echo "    pegasus-plan --submit -s condorpool -o local workflow.yml"
echo "    pegasus-status <submit_dir>"

# ---------------------------------------------------------------------------
# Run the pipeline standalone (no Pegasus) for a quick local smoke test.
# SNOTEL #367 is public (no auth); GLEES needs AmeriFlux creds or --base-csv.
#
#   python bin/fetch_sage_data.py   --config $CONFIG --output output/sage_observations.csv
#   python bin/fetch_glees_data.py  --config $CONFIG --output output/glees_observations.csv
#   python bin/fetch_snotel_data.py --config $CONFIG --output output/snotel_observations.csv
#
#   # Merge every source into one analysis-ready dataset
#   python bin/harmonize.py --inputs output/sage_observations.csv output/glees_observations.csv \
#                                    output/snotel_observations.csv \
#                           --output output/observations.csv --report output/harmonization_report.json
#   OBS=output/observations.csv
#
#   python bin/soil_moisture_map.py    --observations $OBS --config $CONFIG --output output/soil_moisture_map.json
#   python bin/forest_water_stress.py  --observations $OBS --config $CONFIG --output output/forest_water_stress.json
#   python bin/snowmelt_recharge.py    --observations $OBS --config $CONFIG --output output/snowmelt_recharge.json
#   python bin/drought_warning.py      --observations $OBS --soil-moisture output/soil_moisture_map.json \
#                                      --forest-stress output/forest_water_stress.json --config $CONFIG --output output/drought_warning.json
#   python bin/wildfire_risk.py        --observations $OBS --soil-moisture output/soil_moisture_map.json \
#                                      --forest-stress output/forest_water_stress.json --config $CONFIG --output output/wildfire_risk.json
#   python bin/visualize_dashboard.py  --soil-moisture output/soil_moisture_map.json --forest-stress output/forest_water_stress.json \
#                                      --snowmelt output/snowmelt_recharge.json --drought-warning output/drought_warning.json \
#                                      --wildfire output/wildfire_risk.json --config $CONFIG --output output/drought_dashboard.png
# ---------------------------------------------------------------------------
