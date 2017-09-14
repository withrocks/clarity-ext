from __future__ import print_function
import click
import logging
from clarity_ext.service import ProcessService
from clarity_ext import ClaritySession
from clarity_ext import utils
import subprocess
import sys
import requests_cache
import re
import yaml
from clarity_ext.service.routing_service import RerouteInfo, RoutingService


@click.group()
@click.option("--level", default="INFO")
@click.option("--to-file/--no-to-file", default=False)
def main(level, to_file):
    """Provides a limited set of commands for editing or querying the data in the LIMS"""
    log_level = level

    if not to_file:
        logging.basicConfig(level=log_level)
    else:
        logging.basicConfig(level=log_level,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            filename='adhoc.log',
                            filemode='a')

    # NOTE: The executed command is added to the log. Ensure sensitive data is filtered out if added
    # to any of the commands
    logging.info("Executing: {}".format(sys.argv))
    results = subprocess.check_output(["pip", "freeze"])
    for result in results.splitlines():
        if "git+" in result:
            logging.info(result)


@main.command("list-process-types")
@click.option("--contains", help="Filter to process type containing this regex pattern anywhere in the XML")
@click.option("--list-procs", help="Lists procs: all|active")
@click.option("--ui-links", is_flag=True, help="Report ui links rather than api links")
def list_process_types(contains, list_procs, ui_links):
    """Lists all process types in the lims. Uses a cache file (process-type.sqlite)."""
    process_svc = ProcessService(use_cache=True)
    for process_type in process_svc.list_process_types(contains):
        click.echo("{name}: {uri}".format(name=process_type.name, uri=process_type.uri))

        if list_procs is not None:
            if list_procs not in ["all", "active"]:
                raise ValueError("Proc status not supported: {}".format(list_procs))
            for process in process_svc.list_processes_by_process_type(process_type):
                if list_procs == "active" and process.date_run is not None:
                    continue
                uri = process.uri if not ui_links else process_svc.ui_link_process(process)
                click.echo(u" - {}: date_run={}, technician={}".format(uri,
                           process.date_run, process.technician.name))


@main.command("get-stages")
@click.option("--workflow-status", default="ACTIVE")
@click.option("--workflow-name", default=".*")
@click.option("--protocol-name", default=".*")
@click.option("--stage-name", default=".*")
@click.option("--use-cache/--no-use-cache", default=True)
def get_stages(workflow_status, workflow_name, protocol_name, stage_name, use_cache):
    """Expands information about all stages in the filtered workflows"""
    workflow_pattern = re.compile(workflow_name)
    protocol_pattern = re.compile(protocol_name)
    stage_pattern = re.compile(stage_name)

    if use_cache:
        requests_cache.configure("workflow-info")
    session = ClaritySession.create(None)
    workflows = [workflow for workflow in session.api.get_workflows()
                 if workflow.status == workflow_status and workflow_pattern.match(workflow.name)]

    print("workflow\tprotocol\tstage\turi")
    for workflow in workflows:
        for stage in workflow.api_resource.stages:
            if not protocol_pattern.match(stage.protocol.name) or not stage_pattern.match(stage.name):
                continue
            try:
                print("\t".join([stage.workflow.name, stage.protocol.name, stage.name, stage.uri]))
            except AttributeError as e:
                print("# ERROR workflow={}: {}".format(workflow.uri, e.message))


@main.command("move-artifacts-plan")
@click.argument("artifact-ids", nargs=-1)
@click.argument("assign-workflow-name")
@click.argument("assign-stage-name")
def move_artifacts_plan(artifact_ids, assign_workflow_name, assign_stage_name):
    """Creates a 'plan' for moving the artifact-id, reviewing all parameters etc.
     This plan can be manually reviewed/edited and then executed by running `move-artifacts --commit`"""
    session = ClaritySession.create(None)
    routing_service = RoutingService(session)
    plan = routing_service.build_plan(artifact_ids, assign_workflow_name, assign_stage_name)
    click.echo(yaml.safe_dump(plan, default_flow_style=False))


@main.command("move-artifacts")
@click.argument('plan', type=click.File('rb'))
@click.option("--commit/--no-commit", default=False)
def move_artifacts(plan, commit):
    """Reads a plan created by move-artifacts-plan and moves samples accordingly. Nothing is committed by default,
    but the side effects are logged. Add the --commit flag to commit the transfer."""
    plan = yaml.load(plan)
    reroute_infos = plan["reroutes"]
    session = ClaritySession.create(None)
    routing_service = RoutingService(session, commit)
    routing_service.route(reroute_infos)

if __name__ == "__main__":
    main()
