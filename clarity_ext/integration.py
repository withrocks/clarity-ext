from __future__ import print_function
import os
import shutil
import logging
import importlib
import pkgutil
from driverfile import DriverFileIntegrationTests
from clarity_ext.extensions import NoTestsFoundException


logger = logging.getLogger(__name__)


# Creates an integration test config file based on convention
# i.e. position and contents of the script classes themselves.
class ConfigFromConventionProvider(object):

    @classmethod
    def _enumerate_modules(cls, root_name):
        root = importlib.import_module(root_name)
        for loader, module_name, is_pkg in pkgutil.walk_packages(root.__path__):
            try:
                module = loader.find_module(module_name).load_module(module_name)
            except SyntaxError:
                logger.warning("Syntax error in module {}".format(module_name))
            except ImportError:
                logger.warning("ImportError in module {}".format(module_name))

            yield module

    @classmethod
    def _enumerate_extensions(cls, root_pkg):
        for module in cls._enumerate_modules(root_pkg):
            if hasattr(module, "Extension"):
                yield module

    @classmethod
    def get_extension_config(cls, root_pkg):
        for extension in cls._enumerate_extensions(root_pkg):
            # NOTE: For some reason, the root does not get added to the enumerated modules
            entry = dict()
            entry["module"] = "{}.{}".format(root_pkg, extension.__name__)
            yield entry


class IntegrationTestService(object):
    CACHE_NAME = "test_run_cache"

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.CACHE_FULL_NAME = "{}.sqlite".format(self.CACHE_NAME)

    @staticmethod
    def _test_run_directory(config_entry, pid):
        return os.path.join(".", "runs", config_entry["name"], pid, "test-run")

    @staticmethod
    def _test_frozen_directory(config_entry, pid):
        return os.path.join(".", "runs", config_entry["name"], pid, "test-frozen")

    def _validate_run(self, entry):
        if entry["cmd"] == "driverfile":
            test_provider = DriverFileIntegrationTests()
            for test in entry["tests"]:
                run_path = self._test_run_directory(entry, test["pid"])
                frozen_path = self._test_frozen_directory(entry, test["pid"])
                test_provider.validate(run_path, frozen_path, test)

    def _freeze_test(self, entry, test):
        source = self._test_run_directory(entry, test["pid"])

        if not os.path.exists(source):
            raise FreezingBeforeRunning()

        target = self._test_frozen_directory(entry, test["pid"])
        print("Freezing test {} => {}".format(source, target))
        if os.path.exists(target):
            print("Target already exists, removing it")
            shutil.rmtree(target)
        shutil.copytree(source, target)

    def validate(self, module, config):
        """
        Runs the tests on the frozen tests. The idea is that this should run (at least) on every official build,
        thus validating every script against a known state

        :param config:
        :return:
        """
        from clarity_ext.extensions import ExtensionService
        extension_svc = ExtensionService(lambda _: None)
        config_obj = ConfigFromConventionProvider.get_extension_config(module)
        exception_count = 0

        for entry in config_obj:
            module = entry["module"]
            try:
                extension_svc.run_test(config, None, module, False, True, True)
                print("- {}: SUCCESS".format(module))
            except NoTestsFoundException:
                print("- {}: WARNING - No tests were found".format(module))
            except Exception as e:
                # It's OK to use a catch-all exception handler here since this is only used while
                # running tests, so we want to be optimistic and try to run all tests:
                print("- {}: ERROR - {}".format(module, e.message))
                print("  Fresh run:    clarity-ext extension {} test-fresh".format(module))
                print("  Review, then: clarity-ext extension {} freeze".format(module))
                exception_count += 1

        return exception_count


class FreezingBeforeRunning(Exception):
    """Thrown when the user tries to freeze a state before doing an initial run"""
    pass

