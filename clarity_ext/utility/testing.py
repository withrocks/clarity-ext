"""
Various helpers for mocking data quickly, in either unit tests or notebooks.
"""
from clarity_ext.domain import *
from clarity_ext.service.dilution.service import *
from mock import MagicMock
from clarity_ext.context import ExtensionContext


class DilutionTestDataHelper:
    """
    A helper for creating mock containers and artifacts related to Dilution, in as simple a way
    as possible, even for end-users testing things in notebooks, but can also be used in tests.


    """

    def __init__(self, concentration_ref, create_well_order=Container.DOWN_FIRST):
        self.default_source = "source"
        self.default_target = "target"
        self.containers = dict()
        # Default input/output containers used if the user doesn't provide them:

        self.create_container(self.default_source)
        self.create_container(self.default_target)
        self.concentration_unit = DilutionSettings._parse_conc_ref(concentration_ref)
        assert self.concentration_unit is not None
        # TODO: Change the Container domain object so that it can add analytes to the next available position
        self.well_enumerator = self.containers[self.default_source].enumerate_wells(create_well_order)
        self.pairs = list()

    def set_default_containers(self, source_postfix, target_postfix):
        self.default_source = "source{}".format(source_postfix)
        self.default_target = "target{}".format(target_postfix)

    def create_container(self, container_id):
        container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE,
                              container_id=container_id, name=container_id)
        self.containers[container_id] = container
        return container

    def get_container_by_name(self, container_name):
        """Returns a container by name, creating it if it doesn't exist yet"""
        if container_name not in self.containers:
            self.containers[container_name] = self.create_container(container_name)
        return self.containers[container_name]

    def _create_analyte(self, is_input, partial_name, analyte_type=Analyte):
        name = "{}-{}".format("in" if is_input else "out", partial_name)
        ret = analyte_type(api_resource=None, is_input=is_input,
                           id=name, name=name)
        return ret

    def create_pair(self, pos_from=None, pos_to=None, source_container_name=None, target_container_name=None,
                    source_type=Analyte, target_type=Analyte):
        if source_container_name is None:
            source_container = self.default_source
        if target_container_name is None:
            target_container = self.default_target

        source_container = self.get_container_by_name(source_container)
        target_container = self.get_container_by_name(target_container)

        if pos_from is None:
            well = self.well_enumerator.next()
            pos_from = well.position
        if pos_to is None:
            pos_to = pos_from

        name = "FROM:{}".format(pos_from)
        pair = ArtifactPair(self._create_analyte(True, name, source_type),
                            self._create_analyte(False, name, target_type))
        source_container.set_well(pos_from, artifact=pair.input_artifact)
        target_container.set_well(pos_to, artifact=pair.output_artifact)
        self.pairs.append(pair)
        return pair

    def create_dilution_pair(self, conc1, vol1, conc2, vol2, pos_from=None, pos_to=None,
                             source_type=Analyte, target_type=Analyte,
                             source_container_name=None, target_container_name=None):
        """Creates an analyte pair ready for dilution"""
        pair = self.create_pair(pos_from, pos_to,
                                source_type=source_type, target_type=target_type,
                                source_container_name=source_container_name,
                                target_container_name=target_container_name)
        concentration_unit = DilutionSettings.concentration_unit_to_string(self.concentration_unit)
        conc_source_udf = "Conc. Current ({})".format(concentration_unit)
        conc_target_udf = "Target conc. ({})".format(concentration_unit)
        pair.input_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                  "Current sample volume (ul)": vol1})
        pair.output_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                   "Current sample volume (ul)": vol1,
                                                   "Target vol. (ul)": vol2,
                                                   conc_target_udf: conc2,
                                                   "Dil. calc target vol": None,
                                                   "Dil. calc target conc.": None,
                                                   "Dil. calc source vol": None})
        return pair


def mock_context(**kwargs):
    """Creates a mock with the service provided as keyword arguments, filling the rest with MagicMock"""
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
        session = MagicMock()
        step_repo = MagicMock()
        step_repo.all_artifacts = self._all_artifacts
        user = User("Integration", "Tester", "no-reply@medsci.uu.se", "IT")
        step_repo.get_process = MagicMock(return_value=Process(None, "24-1234", user, None, "http://not-avail"))
        os_service = MagicMock()
        file_repository = MagicMock()
        clarity_service = MagicMock()
        process_type = ProcessType(None, None, name="Some process")
        step_repo.current_user = MagicMock(return_value=user)
        step_repo.get_process_type = MagicMock(return_value=process_type)
        self.context = ExtensionContext.create_mocked(session, step_repo, os_service, file_repository, clarity_service)
        # TODO: only mocking this one method of the validation_service for now (quick fix)
        self.context.validation_service.handle_single_validation = MagicMock()

        self._shared_files = list()
        self._analytes = list()

    def logged_validation_results(self):
        return [call[0][0] for call in self.context.validation_service.handle_single_validation.call_args_list]

    def count_logged_validation_results_of_type(self, t):
        return len([result for result in self.logged_validation_results() if type(result) == t])

    def count_logged_validation_results_with_msg(self, msg):
        return len([result for result in self.logged_validation_results()
                    if result.msg == msg])

    def _all_artifacts(self):
        return self._shared_files + self._analytes

    def add_shared_result_file(self, f):
        assert f.name is not None, "You need to supply a name"
        f.id = "92-{}".format(len(self._shared_files))
        self._shared_files.append((None, f))

    def set_user(self, user_name):
        pass

    def add_analyte_pair(self, input, output):
        # TODO: Set id and name if not provided
        self._analytes.append((input, output))

    def add_analyte_pairs(self, pairs):
        self._analytes.extend((pair.input_artifact, pair.output_artifact) for pair in pairs)


class TestExtensionWrapper(object):
    """Similar to TestExtensionContext, but wraps an entire extension"""

    def __init__(self, extension_type):
        self.context_wrapper = TestExtensionContext()
        self.extension = extension_type(self.context_wrapper.context)
