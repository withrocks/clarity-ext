from __future__ import print_function
import click
import logging
from clarity_ext.integration import IntegrationTestService
from clarity_ext.extensions import ExtensionService
from clarity_ext.service.metadata import ExtensionMetadataService
from clarity_ext.tool.template_generator import TemplateNotFoundException, TemplateGenerator
from clarity_ext import ClaritySession
import sys
import time
from clarity_ext.extensions import ResultsDifferFromFrozenData
from clarity_ext.clarity import Configuration


logger = logging.getLogger(__name__)
log_level = None
config = Configuration()


@click.group()
@click.option("--level", default="INFO")
def main(level):
    """
    :param level: ["DEBUG", "INFO", "WARN", "ERROR"]
    :param cache: Set to a cache name if running from a cache (or caching)
                  This is used to ensure reproducible and fast integration tests
    :return:
    """
    global logger
    global log_level
    log_level = level


def default_logging():
    global log_level
    logging.basicConfig(level=log_level)


@main.command()
@click.argument("environment")
def logout(environment):
    clarity_session = ClaritySession()
    clarity_session.logout(environment)


@main.command()
@click.argument("environment")
@click.argument("username", required=False)
@click.argument("password", required=False)
def login(environment, username, password):
    """Log into a clarity environment. This will write login session IDs (cookies) to ~/.clarity-login
    The file will only be readable by the owner. You can log out with logout.
    """
    clarity_session = ClaritySession(config)
    clarity_session.login(environment, username, password)
    click.echo("Successfully logged in and saved auth token to ~/.clarity-ext.user.config")


@main.command("add-environment")
@click.argument("name")
@click.argument("server")
@click.argument("default", type=click.BOOL)
@click.argument("role", type=click.Choice(['dev', 'staging', 'prod']))
def add_environment(name, server, default, role):
    clarity_session = ClaritySession()
    clarity_session.config.set_environment(name, server, default, role)
    clarity_session.config.save()


@main.command()
@click.argument("module")
def validate(module):
    """
    Validates the extension if there exists frozen data for it.
    Can use regex to match extensions.
    """
    default_logging()
    t1 = time.time()
    integration_svc = IntegrationTestService()
    validation_exceptions = integration_svc.validate(module, config)
    delta = time.time() - t1
    if validation_exceptions == 0:
        print("\nAll integration tests ran successfully ({:.3f}s)".format(delta))
    else:
        sys.exit(validation_exceptions)


@main.command("cat")
@click.argument("path")
def cat(path):
    """Prints out a virtual file listed by the ls command"""
    svc = ExtensionMetadataService(config)
    ret = svc.cat(path)
    print(ret)


@main.command("ls")
@click.argument("path")
@click.option("--refresh", type=bool)
def ls(path, refresh):
    """Refreshes the list of running processes in the environment. The environment must be one of the
    environments listed in ./.clarity-ext.config. The path should look like this:

    To list available items for querying:
        clarity-ext ls /<environment>

    To list extensions in a process type:
        clarity-ext ls "/dev/SNP&SEQ Aggregate QC (DNA) v1"

    You can furthermore, for each of those, list all active processes for each of those programs:
        clarity-ext ls --processes active "/dev/SNP&SEQ Aggregate QC (DNA) v1"
    This will list all the extensions with all the active processes.

    All queries are cached in .cache.sqlite3
    """
    from clarity_ext.clarity import NoAuthTokenConfigured
    try:
        svc = ExtensionMetadataService(config)
        for item in svc.ls(path):
            print('"/{}"'.format("/".join(item)))
    except NoAuthTokenConfigured as e:
        click.echo("No auth token has been configured for '{0}'.\n - Run `clarity-ext login {0}` to configure it."
                   .format(e.environment))



@main.command()
@click.argument("module")
@click.argument("mode", required=False)
@click.option("--args")
@click.option("--cache", type=bool)
def extension(module, mode, args, cache):
    """Loads the extension and executes the integration tests.

    :param args: Dynamic parameters to the extension
    """
    global config
    default_logging()

    if mode != "exec":
        click.echo("WARNING: All modes except exec will be deprecated in a future version")

    try:
        if not config:
            config = {
                "test_root_path": "./clarity_ext_scripts/int_tests",
                "frozen_root_path": "./clarity_ext_scripts/int_tests",
                "exec_root_path": "."
            }
            logger.debug("Configuration not provided, using default: {}".format(config))

        # Parse the run arguments list:
        if args and isinstance(args, basestring):
            separated = args.split(" ")
            key_values = (argument.split("=") for argument in separated)
            args = [{key: value for key, value in key_values}]

        validate_against_frozen = True  # Indicates a run that should ignore the frozen directory
        if mode == "test-fresh":
            mode = "test"
            validate_against_frozen = False

        extension_svc = ExtensionService(lambda msg: print(msg))
        if mode == ExtensionService.RUN_MODE_FREEZE:
            extension_svc.run_freeze(config, args, module)
        elif mode == ExtensionService.RUN_MODE_TEST:
            extension_svc.set_log_strategy(log_level, True, False, True)
            try:
                extension_svc.run_test(config, args, module, True, cache, validate_against_frozen)
            except ResultsDifferFromFrozenData as ex:
                print("Results differ from frozen data: " + ex.message)
        elif mode == ExtensionService.RUN_MODE_EXEC:
            extension_svc.set_log_strategy(log_level, False, True, True, "/opt/clarity-ext/logs", "extensions.log")
            extension_svc.run_exec(config, args, module)
        else:
            raise NotImplementedError("Mode '{}' is not implemented".format(mode))
    except Exception as ex:
        if mode == "test":
            # Just re-raise when testing - to keep the stacktrace
            raise
        logger.exception("Exception while running extension")
        msg = "There was an exception while running the extension: '{}'. ".format(ex.message) + \
              "Refer to the file 'Step log' if available."
        if extension_svc.rotating_file_path:
            msg += " The application log is available in {}.".format(extension_svc.rotating_file_path)
        raise Exception(msg)


@main.command()
def templates():
    """
    Lists all available templates
    """
    click.echo("Available templates:")
    template_generator = TemplateGenerator()
    for template in template_generator.list_templates():
        if template.name != "_base":
            click.echo("  {}".format(template))

    click.echo()
    click.echo("Create from template by executing:")
    click.echo("  clarity-ext create <template-name> <package>")


@main.command()
@click.argument("template")
@click.argument("package")
def create(template, package):
    """
    Creates a new extension from a template.
    """
    click.echo("Creating a new '{}' extension in package '{}'...".format(template, package))
    template_generator = TemplateGenerator()
    try:
        template_generator.create(template, package)
    except TemplateNotFoundException:
        click.echo("ERROR: Can't find template called: {}".format(template))


@main.command("fix-pycharm")
@click.argument("package")
def fix_pycharm(package):
    template_generator = TemplateGenerator()
    template_generator.fix_pycharm(package)

if __name__ == "__main__":
    main()

