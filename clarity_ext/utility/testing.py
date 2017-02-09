"""
Various helpers for mocking data quickly, in either unit tests or notebooks.
"""
from clarity_ext.domain import *
from clarity_ext.service.dilution.service import *
from mock import MagicMock


class DilutionTestDataHelper:
    """
    A helper for creating mock containers and artifacts related to Dilution, in as simple a way
    as possible, even for end-users testing things in notebooks, but can also be used in tests.
    """
    def __init__(self, robots, settings, validator,
                 create_well_order=Container.DOWN_FIRST):
        self.input_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE,
                                         container_id="input", name="input")
        self.output_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE,
                                          container_id="output", name="output")
        self.concentration_unit = settings.concentration_ref
        self.well_enumerator = self.input_container.enumerate_wells(create_well_order)
        self.pairs = list()
        self.robots = robots
        self.settings = settings
        self.validator = validator
        dilution_service = DilutionService(validation_service=MagicMock())
        self.session = dilution_service.create_session(self.robots, self.settings, self.validator)

    def _create_analyte(self, is_input, partial_name):
        name = "{}-{}".format("in" if is_input else "out", partial_name)
        ret = Analyte(api_resource=None, is_input=is_input,
                      id=name, name=name)
        return ret

    def evaluate(self):
        self.session.evaluate(self.pairs)
        return self.session

    def create_pair(self, pos_from=None, pos_to=None):
        if pos_from is None:
            well = self.well_enumerator.next()
            pos_from = well.position
        if pos_to is None:
            pos_to = pos_from

        name = "FROM:{}".format(pos_from)
        pair = ArtifactPair(self._create_analyte(True, name),
                            self._create_analyte(False, name))
        self.input_container.set_well(pos_from, artifact=pair.input_artifact)
        self.output_container.set_well(pos_to, artifact=pair.output_artifact)
        self.pairs.append(pair)
        return pair

    def create_dilution_pair(self, conc1, vol1, conc2, vol2, pos_from=None, pos_to=None):
        """Creates an analyte pair ready for dilution"""
        pair = self.create_pair(pos_from, pos_to)
        concentration_unit = DilutionSettings.concentration_unit_to_string(self.concentration_unit)
        conc_source_udf = "Conc. Current ({})".format(concentration_unit)
        conc_target_udf = "Target conc. ({})".format(concentration_unit)
        pair.input_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                  "Current sample volume (ul)": vol1})
        pair.output_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                   "Current sample volume (ul)": vol1,
                                                   "Target vol. (ul)": vol2,
                                                   conc_target_udf: conc2})
        return pair


def mock_context(**kwargs):
    """Creates a mock with the service provided as keyword arguments, filling the rest with MagicMock"""
    from clarity_ext.context import ExtensionContext
    # TODO: Needs to be updated when the signature is updated. Fix that (or use a better approach)
    for arg in ["session", "artifact_service", "file_service", "current_user", "step_logger_service",
                "step_repo", "clarity_service", "dilution_service", "process_service",
                "upload_file_service", "validation_service"]:
        kwargs.setdefault(arg, MagicMock())
    return ExtensionContext(**kwargs)


class TestExtensionContext(object):
    """
    A helper (wrapper) for creating test ExtensionContext objects, which are used for integration tests of the
    type where you want to mock all repositories, but keep the services hooked up as they would be in production.

    Wraps that kind of mocked ExtensionContext and provides various convenience methods for adding data to the mocked
    repositories.

    The idea is that this should be usable by users that have little knowledge about how the framework works.
    """
    def __init__(self):
        from clarity_ext.context import ExtensionContext
        session = MagicMock()
        step_repo = MagicMock()
        step_repo.all_artifacts = self._all_artifacts
        os_service = MagicMock()
        self.context = ExtensionContext.create_mocked(session, step_repo, os_service)
        self._shared_files = list()
        self._analytes = list()

    def _all_artifacts(self):
        return self._shared_files + self._analytes   # TODO: Append others

    def add_shared_result_file(self, f):
        assert f.name is not None, "You need to supply a name"
        f.id = "92-{}".format(len(self._shared_files))
        self._shared_files.append((None, f))

    def add_analyte_pair(self, input, output):
        # TODO: Set id and name if not provided
        self._analytes.append((input, output))


class TestExtensionWrapper(object):
    """Similar to TestExtensionContext, but wraps an entire extension"""
    def __init__(self, extension_type):
        self.context_wrapper = TestExtensionContext()
        self.extension = extension_type(self.context_wrapper.context)

