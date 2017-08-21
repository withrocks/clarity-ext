from __future__ import print_function
import click
import logging
from clarity_ext.service import ProcessService


@click.group()
@click.option("--level", default="INFO")
def main(level):
    """
    :param level: ["DEBUG", "INFO", "WARN", "ERROR"]
    :return:
    """
    log_level = level
    logging.basicConfig(level=log_level)


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


@main.command("move-samples")
@click.argument("workflow")
def move_samples(workflow):
    """Moves all samples that are in a particular workflow from one workflow to another."""
    pass

if __name__ == "__main__":
    main()

