from __future__ import print_function
import click
import logging
import os
import yaml

config = None
logger = logging.getLogger(__name__)
log_level = None


@click.group()
@click.option("--level", default="INFO")
def main(level):
    """
    :param level: ["DEBUG", "INFO", "WARN", "ERROR"]
    :param cache: Set to a cache name if running from a cache (or caching)
                  This is used to ensure reproducible and fast integration tests
    :return:
    """
    global config
    global logger
    global log_level
    log_level = level

    if os.path.exists("clarity-ext.config"):
        with open("clarity-ext.config", "r") as f:
            config = yaml.load(f)


def default_logging():
    global log_level
    logging.basicConfig(level=log_level)


@main.command("system-skeleton")
#@click.argument("package")
def system_skeleton():
    """
    Creates a new extension from a template.
    """
    click.echo("TODO: Create a skeleton")


@main.command("system-test")
@click.argument("spec")
def system_test(spec):
    print("Run a system test according to the spec")
    import yaml
    with open(spec) as fs:
        x = yaml.load(fs)
    import pprint
    pprint.pprint(x)
    print("Spec name: '{}'".format(x["name"]))
    print("Find the step and see if we can work with the queue")
    # https://genologics.zendesk.com/hc/en-us/articles/213976423-Starting-a-Protocol-Step-via-the-API

    # 1. query the protocols endpoint, find the protocol with the name we're interested in
    print("Searching /protocols for name={}".format(x["select"]["step"]))
    from clarity_ext.service.process_service import ProcessService
    process_svc = ProcessService()
    process_svc.get_queue(protocol="TestOnly-dev-protocol1", step="TestOnly - steinar - Logging")




    # TODO: https://genologics.zendesk.com/hc/en-us/articles/213975523-Assigning-Samples-to-Workflows




if __name__ == "__main__":
    main()
