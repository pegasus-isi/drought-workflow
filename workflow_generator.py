#!/usr/bin/env python3

"""Drought Workflow Generator for Pegasus WMS.

Builds a workflow that turns SAGE (Waggle edge sensors) and GLEES (AmeriFlux
US-GLE flux tower) observations into five decision layers:

    1. Soil moisture map
    2. Forest water stress map
    3. Snowmelt recharge timing
    4. Drought warning
    5. Wildfire risk

DAG shape:

    fetch_sage  ─┐
                 ├─> soil_moisture_map ──┐
    fetch_glees ─┤                       ├─> wildfire_risk ───┐
                 ├─> forest_water_stress ┤                    ├─> dashboard
                 │                       └─> drought_warning ─┤
                 └─> snowmelt_recharge ───────────────────────┘

Usage:
    ./workflow_generator.py --config region_config.json -o workflow.yml

    # A centrally hosted site catalog, or a plain HTCondor pool:
    ./workflow_generator.py --config region_config.json -s unity.yml
    ./workflow_generator.py --sources snotel -e condorpool

Sites follow pegasus-isi/pegasus-gromacs: jobs run on a site named "compute",
defined by a centrally hosted site catalog (-s access-pegasus.yml, ...;
https://github.com/pegasushub/pegasus-site-catalogs) or by one in
~/.pegasusrc. The generator writes no site catalog and never submits; it
prints the pegasus-plan command. Drought-Workflow.ipynb drives the same class
interactively.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DroughtWorkflow:
    """Generate the Pegasus workflow for the drought monitoring layers."""

    wf = sc = tc = rc = props = None
    wf_name = "drought"

    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        self.local_storage_dir = os.path.join(self.wf_dir, "output")

    def write(self):
        if self.sc is not None:
            self.sc.write()
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write(file=self.dagfile)

    # ------------------------------------------------------------------
    # Plan / run / monitor (thin wrappers over the Pegasus API Workflow
    # object, for interactive use e.g. from a Jupyter notebook)
    # ------------------------------------------------------------------
    def plan_submit(self, exec_site_name="compute", raise_errors=False):
        try:
            self.wf.plan(
                dir="submit",
                sites=[exec_site_name],
                output_sites=["local"],
                cleanup="none",
                verbose=1,
                submit=True,
            )
        except PegasusClientError as e:
            print(e)
            if raise_errors:
                raise

    def status(self):
        try:
            self.wf.status(long=True)
        except PegasusClientError as e:
            print(e)

    def wait(self):
        try:
            self.wf.wait()
        except PegasusClientError as e:
            print(e)

    def statistics(self):
        try:
            self.wf.statistics()
        except PegasusClientError as e:
            print(e)

    def create_pegasus_properties(self, hosted_site_catalog=None):
        self.props = Properties()
        self.props["pegasus.transfer.threads"] = "16"
        if hosted_site_catalog:
            # Use one of Pegasus' centrally hosted site catalogs instead of
            # a locally generated one. pegasus-plan downloads and caches the
            # named file from the catalog repository at plan time.
            # https://pegasus.isi.edu/documentation/reference-guide/catalogs.html#centrally-hosted-site-catalogs
            self.props["pegasus.catalog.site.repo.file"] = hosted_site_catalog

    # ------------------------------------------------------------------
    # Site Catalog
    #
    # Not used by the CLI below by default — pegasus-plan resolves the site
    # catalog from a centrally hosted one instead (see -s/--hosted-site-catalog
    # and create_pegasus_properties above). Kept for programmatic/notebook use
    # when a self-contained, locally generated HTCondor site catalog is wanted.
    # ------------------------------------------------------------------
    def create_sites_catalog(self, exec_site_name="compute"):
        self.sc = SiteCatalog()

        local = Site("local").add_directories(
            Directory(
                Directory.SHARED_SCRATCH, self.shared_scratch_dir
            ).add_file_servers(
                FileServer("file://" + self.shared_scratch_dir, Operation.ALL)
            ),
            Directory(
                Directory.LOCAL_STORAGE, self.local_storage_dir
            ).add_file_servers(
                FileServer("file://" + self.local_storage_dir, Operation.ALL)
            ),
        )

        exec_site = (
            Site(exec_site_name)
            .add_condor_profile(universe="vanilla")
            .add_pegasus_profile(style="condor")
        )

        self.sc.add_sites(local, exec_site)

    def create_replica_catalog(self):
        logger.info("Creating replica catalog")
        self.rc = ReplicaCatalog()

    def create_transformation_catalog(self, container_sif,
                                      exec_site_name="compute"):
        """Container and transformations, registered on the execution site;
        the .sif lives on "local" (the submit host) and is staged."""
        logger.info("Creating transformation catalog")
        self.tc = TransformationCatalog()
        # A local Apptainer .sif built with `apptainer build`. Pegasus stages
        # the file like any other input, so image_site is the site where the
        # .sif physically lives (the submit host = "local").
        sif_path = (
            container_sif
            if os.path.isabs(container_sif)
            else os.path.join(self.wf_dir, container_sif)
        )
        if not os.path.exists(sif_path):
            logger.warning(
                "Apptainer image not found at %s — build it first with: "
                "apptainer build %s Apptainer/Drought_Container.def",
                sif_path, sif_path)
        container = Container(
            "drought_container",
            container_type=Container.SINGULARITY,
            image="file://" + sif_path,
            image_site="local",
        )
        self.tc.add_containers(container)

        # (name, relative path, memory)
        specs = [
            ("fetch_sage_data", "bin/fetch_sage_data.py", "1 GB"),
            # AmeriFlux BASE is a single multi-decade half-hourly CSV (US-GLE
            # spans 1999-2025); reading it whole peaks well above 2 GB.
            ("fetch_glees_data", "bin/fetch_glees_data.py", "8 GB"),
            ("fetch_snotel_data", "bin/fetch_snotel_data.py", "1 GB"),
            ("harmonize", "bin/harmonize.py", "2 GB"),
            ("soil_moisture_map", "bin/soil_moisture_map.py", "2 GB"),
            ("forest_water_stress", "bin/forest_water_stress.py", "2 GB"),
            ("snowmelt_recharge", "bin/snowmelt_recharge.py", "2 GB"),
            ("drought_warning", "bin/drought_warning.py", "2 GB"),
            ("wildfire_risk", "bin/wildfire_risk.py", "2 GB"),
            ("visualize_dashboard", "bin/visualize_dashboard.py", "2 GB"),
        ]
        for name, rel, mem in specs:
            self.tc.add_transformations(
                Transformation(
                    name,
                    site=exec_site_name,
                    pfn=os.path.join(self.wf_dir, rel),
                    is_stageable=True,
                    container=container,
                ).add_pegasus_profile(memory=mem)
            )

    def create_workflow(self, args):
        logger.info("Creating workflow")
        self.wf = Workflow(self.wf_name)

        # Inputs.
        config = File(os.path.basename(args.config))
        self.rc.add_replica("local", config, os.path.abspath(args.config))
        common = File("drought_common.py")
        self.rc.add_replica(
            "local", common, os.path.join(self.wf_dir, "bin/drought_common.py")
        )

        # Intermediate / output files.
        observations = File("observations.csv")  # harmonized, single layer input
        harmonize_report = File("harmonization_report.json")
        sm_json = File("soil_moisture_map.json")
        fws_json = File("forest_water_stress.json")
        snow_json = File("snowmelt_recharge.json")
        drought_json = File("drought_warning.json")
        fire_json = File("wildfire_risk.json")
        dashboard = File("drought_dashboard.png")

        dates = []
        if args.start_date:
            dates += ["--start-date", args.start_date]
        if args.end_date:
            dates += ["--end-date", args.end_date]

        # --- Fetch jobs (only for the selected sources) ---
        # Each fetcher normalises its source onto the common schema; harmonize
        # merges whichever ones are present. Selecting a subset lets you run,
        # e.g., SNOTEL-only for a quick public-data test with no credentials.
        fetch_jobs = []
        source_obs = []

        if "sage" in args.sources:
            sage_obs = File("sage_observations.csv")
            fetch_sage = Job("fetch_sage_data", node_label="fetch_sage")
            fetch_sage.add_args("--config", config, *dates, "--output", sage_obs)
            fetch_sage.add_inputs(config)
            fetch_sage.add_outputs(sage_obs, stage_out=True, register_replica=False)
            fetch_sage.add_dagman_profile(retry="2")  # retry transient API failures
            self.wf.add_jobs(fetch_sage)
            fetch_jobs.append(fetch_sage)
            source_obs.append(sage_obs)

        if "glees" in args.sources:
            glees_obs = File("glees_observations.csv")
            fetch_glees = Job("fetch_glees_data", node_label="fetch_glees")
            fetch_glees.add_args("--config", config, *dates, "--output", glees_obs)
            fetch_glees.add_inputs(config)
            if args.glees_base_csv:
                base_csv = File(os.path.basename(args.glees_base_csv))
                self.rc.add_replica(
                    "local", base_csv, os.path.abspath(args.glees_base_csv)
                )
                fetch_glees.add_args("--base-csv", base_csv)
                fetch_glees.add_inputs(base_csv)
            else:
                # HTCondor runs jobs in a clean environment, so AmeriFlux
                # credentials exported in the submit shell do NOT reach the job.
                # Capture them at generation time and pass them into the job's
                # environment (they travel into the container too).
                amf_user = os.environ.get("AMERIFLUX_USER_ID")
                amf_email = os.environ.get("AMERIFLUX_USER_EMAIL")
                if amf_user and amf_email:
                    fetch_glees.add_env(
                        AMERIFLUX_USER_ID=amf_user,
                        AMERIFLUX_USER_EMAIL=amf_email,
                    )
                else:
                    logger.warning(
                        "GLEES selected but AMERIFLUX_USER_ID / "
                        "AMERIFLUX_USER_EMAIL are not set in this environment and "
                        "no --glees-base-csv was given. Export the credentials "
                        "before generating, or pass --glees-base-csv; otherwise "
                        "the fetch_glees job will fail."
                    )
            fetch_glees.add_outputs(glees_obs, stage_out=True, register_replica=False)
            fetch_glees.add_dagman_profile(retry="2")  # retry transient API failures
            self.wf.add_jobs(fetch_glees)
            fetch_jobs.append(fetch_glees)
            source_obs.append(glees_obs)

        if "snotel" in args.sources:
            snotel_obs = File("snotel_observations.csv")
            fetch_snotel = Job("fetch_snotel_data", node_label="fetch_snotel")
            fetch_snotel.add_args("--config", config, *dates, "--output", snotel_obs)
            fetch_snotel.add_inputs(config)
            fetch_snotel.add_outputs(snotel_obs, stage_out=True, register_replica=False)
            fetch_snotel.add_dagman_profile(retry="2")  # retry transient API failures
            self.wf.add_jobs(fetch_snotel)
            fetch_jobs.append(fetch_snotel)
            source_obs.append(snotel_obs)

        # --- Harmonize: merge the selected sources into one analysis-ready dataset ---
        harmonize = Job("harmonize", node_label="harmonize")
        harmonize.add_args(
            "--inputs", *source_obs,
            "--output", observations, "--report", harmonize_report,
        )
        harmonize.add_inputs(common, *source_obs)
        harmonize.add_outputs(observations, stage_out=True, register_replica=False)
        harmonize.add_outputs(harmonize_report, stage_out=True, register_replica=False)
        self.wf.add_jobs(harmonize)
        for f in fetch_jobs:
            self.wf.add_dependency(f, children=[harmonize])

        # --- Layer jobs that consume the harmonized observations ---
        sm_job = self._layer(
            "soil_moisture_map", "sm",
            ["--observations", observations, "--config", config, "--output", sm_json],
            [common, config, observations], sm_json,
        )
        fws_job = self._layer(
            "forest_water_stress", "fws",
            ["--observations", observations, "--config", config, "--output", fws_json],
            [common, config, observations], fws_json,
        )
        snow_job = self._layer(
            "snowmelt_recharge", "snow",
            ["--observations", observations, "--config", config, "--output", snow_json],
            [common, config, observations], snow_json,
        )
        for j in (sm_job, fws_job, snow_job):
            self.wf.add_dependency(harmonize, children=[j])

        # --- Composite layers that depend on soil-moisture + forest-stress ---
        drought_job = self._layer(
            "drought_warning", "drought",
            ["--observations", observations,
             "--soil-moisture", sm_json, "--forest-stress", fws_json,
             "--config", config, "--output", drought_json],
            [common, config, observations, sm_json, fws_json], drought_json,
        )
        fire_job = self._layer(
            "wildfire_risk", "fire",
            ["--observations", observations,
             "--soil-moisture", sm_json, "--forest-stress", fws_json,
             "--config", config, "--output", fire_json],
            [common, config, observations, sm_json, fws_json], fire_json,
        )
        for j in (drought_job, fire_job):
            self.wf.add_dependency(sm_job, children=[j])
            self.wf.add_dependency(fws_job, children=[j])

        # --- Dashboard depends on every layer ---
        viz = Job("visualize_dashboard", node_label="dashboard")
        viz.add_args(
            "--soil-moisture", sm_json, "--forest-stress", fws_json,
            "--snowmelt", snow_json, "--drought-warning", drought_json,
            "--wildfire", fire_json, "--config", config, "--output", dashboard,
        )
        viz.add_inputs(sm_json, fws_json, snow_json, drought_json, fire_json, config)
        viz.add_outputs(dashboard, stage_out=True, register_replica=False)
        self.wf.add_jobs(viz)
        for j in (sm_job, fws_job, snow_job, drought_job, fire_job):
            self.wf.add_dependency(j, children=[viz])

    def _layer(self, transformation, label, arg_list, inputs, output):
        """Create a layer job, add it to the workflow, and return it."""
        job = Job(transformation, node_label=label)
        job.add_args(*arg_list)
        job.add_inputs(*inputs)
        job.add_outputs(output, stage_out=True, register_replica=False)
        self.wf.add_jobs(job)
        return job


def main():
    parser = argparse.ArgumentParser(
        description="Generate Pegasus workflow for drought / wildfire monitoring"
    )
    parser.add_argument("--config", default="region_config.json",
                        help="Region/sensor configuration JSON")
    parser.add_argument("--sources", nargs="+", default=["sage", "glees", "snotel"],
                        choices=["sage", "glees", "snotel"],
                        help="Data sources to fetch (default: all). Use a subset "
                             "to run with only some sources, e.g. --sources snotel "
                             "for a credential-free public-data test.")
    parser.add_argument("--start-date", help="Override start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Override end date (YYYY-MM-DD)")
    parser.add_argument("--glees-base-csv",
                        help="Optional pre-downloaded AmeriFlux BASE CSV "
                             "(skips the credentialed download)")
    parser.add_argument("-s", "--hosted-site-catalog", metavar="FILE",
                        type=str, default=None,
                        help="Name of a Pegasus centrally hosted site catalog "
                             "to plan against (e.g. access-pegasus.yml), "
                             "instead of a locally generated one. Sets "
                             "pegasus.catalog.site.repo.file; see "
                             "https://pegasus.isi.edu/documentation/"
                             "reference-guide/catalogs.html"
                             "#centrally-hosted-site-catalogs")
    # --execution-site is kept as an alias: earlier releases used that name.
    parser.add_argument("-e", "--execution-site-name", "--execution-site",
                        dest="execution_site_name", metavar="STR",
                        default="compute",
                        help="Execution site name (default: compute; "
                             "condorpool on a plain HTCondor pool with no "
                             "site catalog)")
    parser.add_argument("--container-sif",
                        default="Apptainer/Drought_Container.sif",
                        help="Path to the Apptainer .sif image, absolute or "
                             "relative to the workflow directory")
    parser.add_argument("-o", "--output", default="workflow.yml",
                        help="Output workflow file")
    args = parser.parse_args()

    # De-duplicate while keeping a stable fetch order.
    args.sources = [s for s in ("sage", "glees", "snotel") if s in args.sources]

    if not os.path.exists(args.config):
        logger.error("Config file not found: %s", args.config)
        sys.exit(1)

    try:
        wf = DroughtWorkflow(dagfile=args.output)

        wf.create_pegasus_properties(
            hosted_site_catalog=args.hosted_site_catalog)
        wf.create_replica_catalog()
        wf.create_transformation_catalog(
            container_sif=args.container_sif,
            exec_site_name=args.execution_site_name)
        wf.create_workflow(args)
        wf.write()

        logger.info("\n" + "=" * 70)
        logger.info("WORKFLOW GENERATION COMPLETE")
        logger.info("=" * 70)
        logger.info("  Workflow file: %s", args.output)
        logger.info("  Sources: %s", ", ".join(args.sources))
        logger.info("  Layers: soil moisture, forest water stress, snowmelt, "
                    "drought warning, wildfire risk")
        logger.info("\nNext steps:")
        logger.info("  1. Review workflow: %s", args.output)
        logger.info("  Execution site: %s", args.execution_site_name)
        logger.info("  Hosted site catalog: %s", args.hosted_site_catalog
                    or "(none — supply your own site catalog)")
        logger.info("  2. Plan and submit: pegasus-plan --dir submit -s %s "
                    "-o local --submit %s",
                    args.execution_site_name, args.output)
        logger.info("  3. Monitor: pegasus-status <submit_dir>")
        logger.info("=" * 70 + "\n")
    except Exception as exc:
        logger.error("Failed to generate workflow: %s", exc)
        raise


if __name__ == "__main__":
    main()
