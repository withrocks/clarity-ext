from __future__ import print_function
import sys
import click
import logging
from clarity_ext.integration import IntegrationTestService
from clarity_ext.extensions import ExtensionService
from clarity_ext.tool.template_generator import TemplateNotFoundException, TemplateGenerator
import os
import yaml
import time
from clarity_ext.extensions import ResultsDifferFromFrozenData

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


if __name__ == "__main__":
    main()
