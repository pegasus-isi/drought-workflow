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

    # A Slurm cluster:
    ./workflow_generator.py --config region_config.json --site-style slurm \\
        --queue cpu --project my_lab

Sites: the workflow names no scheduler. Jobs state memory and a wall-clock
runtime, and plan against a site named "compute". A hosted site catalog
(-s unity.yml, ...) defines it; otherwise custom_sites.py writes it to
sites.yml as an HTCondor pool, or for Slurm via --site-style slurm --queue
--project.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from Pegasus.api import *

# Site-catalog handling. Keeps this generator free of scheduler details.
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from custom_sites import (  # noqa: E402
    HOSTED_SITE, STYLES, ensure_sites_yml, hosted_catalog, is_batch_site, parse_profile,
    parse_tag_profile, worker_package_url,
)

# The container's OS, for the Pegasus worker package staged into it. Must
# match the image's base (Apptainer/Drought_Container.def:
# python:3.11-slim-trixie, Debian 13), not the submit host.
CONTAINER_PLATFORM = "x86_64_deb_13"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DroughtWorkflow:
    """Generate the Pegasus workflow for the drought monitoring layers."""

    wf = tc = rc = props = None
    wf_name = "drought"
    # Set by main() from the site in use (see the site-catalog block there).
    worker_package_url = None
    bind_workflow_dir = False

    def __init__(self, dagfile="workflow.yml"):
        self.dagfile = dagfile
        self.wf_dir = str(Path(__file__).parent.resolve())
        self.shared_scratch_dir = os.path.join(self.wf_dir, "scratch")
        self.local_storage_dir = os.path.join(self.wf_dir, "output")

    def write(self):
        self.props.write()
        self.rc.write()
        self.tc.write()
        self.wf.write(file=self.dagfile)

    def create_pegasus_properties(self, sites_yml="sites.yml",
                                  bypass_input_staging=False,
                                  hosted_site_catalog=None):
        self.props = Properties()
        self.props["pegasus.transfer.threads"] = "16"
        # Symlink rather than copy when an input already sits on the
        # execution site. A no-op otherwise, so always on.
        self.props["pegasus.transfer.links"] = "true"
        if hosted_site_catalog:
            # A centrally hosted site catalog (pegasushub
            # pegasus-site-catalogs) defines the execution site; pegasus-plan
            # downloads and caches it, and merges sites.yml over it.
            self.props["pegasus.catalog.site.repo.file"] = hosted_site_catalog
        if bypass_input_staging:
            # Jobs read inputs (notably the .sif) straight from the submit
            # host's paths. Only valid where workers share a filesystem with
            # the submit host (a Slurm cluster) — never on a condor pool
            # staging over HTCondor file transfer.
            self.props["pegasus.transfer.bypass.input.staging"] = "true"
        if self.worker_package_url:
            # Stage the container-compatible worker package named in the TC
            # (pegasus::worker) and never download one from inside a job:
            # the image may lack curl/wget, workers may lack internet, and the
            # submit host's kickstart may need a newer glibc than the image.
            self.props["pegasus.transfer.worker.package"] = "true"
            self.props["pegasus.transfer.worker.package.strict"] = "false"
            self.props["pegasus.transfer.worker.package.autodownload"] = "false"
        if os.path.isfile(sites_yml):
            # Lets pegasus-plan find sites.yml from any directory.
            self.props["pegasus.catalog.site"] = "YAML"
            self.props["pegasus.catalog.site.file"] = os.path.abspath(sites_yml)

    def create_replica_catalog(self):
        logger.info("Creating replica catalog")
        self.rc = ReplicaCatalog()

    def create_transformation_catalog(self, container_sif):
        """Container and transformations. Nothing here names a site: the
        scripts and the .sif live on "local" (the submit host) and are
        shipped to wherever the job runs."""
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
        if self.bind_workflow_dir:
            # Batch sites stage inputs as symlinks into the workflow
            # directory, and PegasusLite starts containers with --no-home, so
            # without this bind every job fails with kickstart "Unable to
            # execute the specified binary" (exit 127). Never on a condor
            # pool: the directory does not exist on its workers.
            container.add_pegasus_profile(
                container_arguments=f"--bind {self.wf_dir}")
        self.tc.add_containers(container)

        # (name, relative path, memory, runtime). runtime is a wall-clock
        # budget in seconds; batch sites (Slurm) refuse or kill jobs without
        # one, so keep it generous — fetch jobs include API retries/backoff.
        specs = [
            ("fetch_sage_data", "bin/fetch_sage_data.py", "1 GB", 3600),
            # AmeriFlux BASE is a single multi-decade half-hourly CSV (US-GLE
            # spans 1999-2025); reading it whole peaks well above 2 GB.
            ("fetch_glees_data", "bin/fetch_glees_data.py", "8 GB", 3600),
            ("fetch_snotel_data", "bin/fetch_snotel_data.py", "1 GB", 3600),
            ("harmonize", "bin/harmonize.py", "2 GB", 1800),
            ("soil_moisture_map", "bin/soil_moisture_map.py", "2 GB", 1800),
            ("forest_water_stress", "bin/forest_water_stress.py", "2 GB", 1800),
            ("snowmelt_recharge", "bin/snowmelt_recharge.py", "2 GB", 1800),
            ("drought_warning", "bin/drought_warning.py", "2 GB", 1800),
            ("wildfire_risk", "bin/wildfire_risk.py", "2 GB", 1800),
            ("visualize_dashboard", "bin/visualize_dashboard.py", "2 GB", 1800),
        ]
        for name, rel, mem, runtime in specs:
            self.tc.add_transformations(
                Transformation(
                    name,
                    site="local",
                    pfn=os.path.join(self.wf_dir, rel),
                    is_stageable=True,
                    container=container,
                ).add_pegasus_profile(memory=mem, runtime=runtime)
            )
        if self.worker_package_url:
            self.tc.add_transformations(
                Transformation(
                    "worker",
                    namespace="pegasus",
                    site="local",
                    pfn=self.worker_package_url,
                    is_stageable=True,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                )
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
    # Site options. Every one has a default, so a run with only --config
    # plans on "compute": the hosted catalog's site when one is named (-s or
    # ~/.pegasusrc), else an HTCondor pool written to sites.yml. dest
    # "execution_site" is what Studio matches to keep its "Where it runs"
    # choice in sync.
    parser.add_argument("-e", "--execution-site", "--execution-site-name",
                        dest="execution_site", default=HOSTED_SITE,
                        help=f"Site to plan against (default: {HOSTED_SITE!r}, "
                             "the name hosted catalogs give their site)")
    parser.add_argument("-s", "--hosted-site-catalog", metavar="FILE",
                        help="Centrally hosted site catalog to plan against, "
                             "e.g. unity.yml (github.com/pegasushub/"
                             "pegasus-site-catalogs); written to "
                             "pegasus.properties. Default: the one named in "
                             "~/.pegasusrc, if any.")
    parser.add_argument("--site-style",
                        choices=("auto",) + STYLES + ("none",), default="auto",
                        help="auto (default): keep a sites.yml entry or hosted "
                             "catalog if one exists, else add an HTCondor "
                             "site. condor/slurm: (re)write this site's "
                             "entry. none: leave sites.yml alone.")
    parser.add_argument("--queue", metavar="PARTITION",
                        help="Batch partition/queue jobs submit to")
    parser.add_argument("--project", metavar="ACCOUNT",
                        help="Allocation/account charged on a batch site")
    parser.add_argument("--site-scratch", metavar="DIR",
                        help="Slurm: shared scratch visible to workers and "
                             "the submit host (default: ./work)")
    parser.add_argument("--site-profile", action="append", default=[],
                        type=parse_profile, metavar="NS:KEY=VALUE",
                        help="Extra profile on the execution site; repeatable")
    parser.add_argument("--tag-profile", action="append", default=[],
                        type=parse_tag_profile, metavar="TAG:NS:KEY=VALUE",
                        help="Profile for jobs carrying a tag; repeatable")
    parser.add_argument("--shared-filesystem", choices=("auto", "yes", "no"),
                        default="auto",
                        help="Let jobs read inputs directly from the submit "
                             "host. auto: on for Slurm sites, off for HTCondor.")
    parser.add_argument("--sites-yml", metavar="FILE", default="sites.yml",
                        help="Local site catalog (default: sites.yml)")
    parser.add_argument("--skip-sites-catalog", action="store_true",
                        help="Deprecated: same as --site-style none")
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

        # Site catalog and the settings that depend on the site
        if args.skip_sites_catalog:
            args.site_style = "none"
        action, style = ensure_sites_yml(
            args.sites_yml, args.execution_site, wf.wf_dir,
            style=args.site_style, hosted=args.hosted_site_catalog,
            queue=args.queue, project=args.project,
            scratch=args.site_scratch, profiles=args.site_profile,
            tag_profiles=args.tag_profile)
        hosted = hosted_catalog(args.hosted_site_catalog)
        logger.info("Site catalog: %s: %s%s", args.sites_yml, action,
                    f" (merged over hosted {hosted})" if hosted else "")
        if (style is None and hosted and args.execution_site != "local"
                and args.execution_site != HOSTED_SITE):
            # Nothing was written for this site, so planning works only if
            # the hosted catalog happens to define it.
            logger.warning(
                "%r is not defined in %s and hosted catalogs normally define "
                "only %r: pegasus-plan will fail unless %s has it. Use -e %s, "
                "or --site-style condor/slurm to describe %r.",
                args.execution_site, args.sites_yml, HOSTED_SITE, hosted,
                HOSTED_SITE, args.execution_site)
        if args.shared_filesystem == "auto":
            bypass = style is not None and style != "condor"
        else:
            bypass = args.shared_filesystem == "yes"
        wf.bind_workflow_dir = bypass or is_batch_site(
            style, args.hosted_site_catalog)
        wf.worker_package_url = worker_package_url(CONTAINER_PLATFORM)
        if not wf.worker_package_url:
            logger.warning("pegasus-version not found: Pegasus will choose the "
                           "container's worker package itself (needs curl/wget "
                           "in the image and internet on the workers)")

        wf.create_pegasus_properties(
            sites_yml=args.sites_yml, bypass_input_staging=bypass,
            hosted_site_catalog=args.hosted_site_catalog)
        wf.create_replica_catalog()
        wf.create_transformation_catalog(container_sif=args.container_sif)
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
        logger.info("  2. Submit: pegasus-plan --submit -s %s -o local %s",
                    args.execution_site, args.output)
        logger.info("  3. Monitor: pegasus-status <submit_dir>")
        logger.info("=" * 70 + "\n")
    except Exception as exc:
        logger.error("Failed to generate workflow: %s", exc)
        raise


if __name__ == "__main__":
    main()
