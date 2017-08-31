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


@main.command("move-artifacts")
@click.argument("artifact-name")
@click.argument("unassign-stage-name")
@click.argument("assign-workflow-name")
@click.argument("assign-stage-name")
@click.option("--commit/--no-commit", default=False)
def move_artifacts(artifact_name, unassign_stage_name, assign_workflow_name, assign_stage_name, commit):
    """Moves all samples that are in a particular workflow from one workflow to another."""
    # TODO: Currently removes it from all stages it's currently in and assigns it to only one
    # TODO: This is a quick fix, so all the logic is currently in the CLI
    from clarity_ext.service.routing_service import RerouteInfo, RoutingService

    session = ClaritySession.create(None)
    logging.info("Searching for analytes of type '{}'".format(artifact_name))
    artifacts = session.api.get_artifacts(name=artifact_name, type="Analyte")

    def stage_to_detailed_string(stage):
        """Returns a details for the stage. Note that it loads potentially three different resources."""
        return "'{}'({}) / '{}' / '{}'".format(stage.workflow.name, stage.workflow.status, stage.protocol.name, stage.step.name)

    # If there is only one artifact, that's the one we should unqueue, but we're always checking if it's staged
    # before:
    if len(artifacts) > 1:
        logging.info("Found more than one artifact")

    def get_artifacts_queued_for_stage():
        for artifact in artifacts:
            queued_stages = [stage for stage, status, name in artifact.workflow_stages_and_statuses
                             if status == "QUEUED"]  # and name == unassign_stage_name]
            logging.info("Artifact {} is queued for the following stages:".format(artifact.id))
            for stage in queued_stages:
                logging.info("  {}".format(stage_to_detailed_string(stage)))

            # Limit to those matching unassign_stage_name
            if len(queued_stages) > 0:
                yield artifact, queued_stages

    try:
        artifacts_and_stages = get_artifacts_queued_for_stage()
    except ValueError:
        logging.error("Can't find a single artifact with name '{}' that's queued for stage with name '{}'".format(
            artifact_name, unassign_stage_name))
        return

    def log_action(action, artifact, stage):
        logging.info("{} {} Stage({})".format(action, artifact.id, stage_to_detailed_string(stage)))

    reroute_infos = list()
    for artifact, queued_stages in artifacts_and_stages:
        if len(queued_stages) > 1:
            # The artifact is queued in several stages because of a bug that has been reported to Illumina
            logging.info("The artifact is queued in more than one stage. It will be unassigned from all of them.")

        try:
            assign_workflow = utils.single(session.api.get_workflows(name=assign_workflow_name))
        except ValueError:
            logging.error("Not able to find one workflow with name {}".format(assign_workflow_name))
            return

        try:
            assign_stage = utils.single([stage for stage in assign_workflow.stages if stage.name == assign_stage_name])
        except ValueError:
            logging.error("Not able to find one stage with name {} in {}".format(assign_stage_name, assign_workflow_name))

        # Report for which artifacts to remove. This should be reviewed by an RE and then pushed back into this tool.
        reroute_info = RerouteInfo(artifact, queued_stages, [assign_stage])
        # Log the details of the reroute info:
        # NOTE: This loads a lot of resources, but can be good to have.
        logging.info("About to reroute artifact {}, '{}'".format(artifact.id, artifact.name))

        for assign in reroute_info.assign:
            log_action("Assign:", reroute_info.artifact, assign)

        for unassign in reroute_info.unassign:
            log_action("Unassign:", reroute_info.artifact, unassign)
        reroute_infos.append(reroute_info)

    if len(reroute_infos) > 0:
        routing_service = RoutingService(session, commit=commit)
        routing_service.route(reroute_infos)


if __name__ == "__main__":
    main()
