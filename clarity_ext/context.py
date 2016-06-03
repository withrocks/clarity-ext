from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import *
import requests
import os
from clarity_ext.dilution import *
import re
import shutil
import clarity_ext.utils as utils
from lxml import objectify
from clarity_ext import UnitConversion
from clarity_ext.result_file import ResultFile
from clarity_ext.utils import lazyprop


class ExtensionContext(object):
    """
    Defines context objects for extensions.
    """
    def __init__(self, logger=None, cache=False, session=None, step_input_output_repo=None):
        """
        Initializes the context.

        :param logger: A logger instance
        :param cache: Set to True to use the cache folder (.cache) for downloaded files
        :param session: An object encapsulating the connection to Clarity
        :param step_input_output_repo: A repository for accessing input_output maps in the current step

        TODO: The context should fetch everything through (easily mockable) repositories,
        and not use the API directly, so the clarity_svc object can be removed
        """
        self.session = session
        self.advanced = Advanced(session.api)
        self.logger = logger or logging.getLogger(__name__)
        self._local_shared_files = []
        self.cache = cache

        self.units = UnitConversion(self.logger)
        self._update_queue = []
        self.current_step = session.current_step
        self.step_input_output_repo = step_input_output_repo

    def local_shared_file(self, file_name, mode='r'):
        """
        Downloads the local shared file and returns an open file-like object.

        If the file already exists, it will not be downloaded again.

        Details:
        The downloaded files will be removed when the context is cleaned up. This ensures
        that the LIMS will not upload them by accident
        """

        # Ensure that the user is only sending in a "name" (alphanumerical or spaces)
        # File paths are not allowed
        if not re.match(r"[\w ]+", file_name):
            raise ValueError("File name can only contain alphanumeric characters, underscores and spaces")
        local_file_name = file_name.replace(" ", "_")
        local_path = os.path.abspath(local_file_name)
        local_path = os.path.abspath(local_path)
        cache_directory = os.path.abspath(".cache")
        cache_path = os.path.join(cache_directory, local_file_name)

        if self.cache and os.path.exists(cache_path):
            self.logger.info("Fetching cached artifact from '{}'".format(cache_path))
            shutil.copy(cache_path, ".")
        else:
            if not os.path.exists(local_path):
                by_name = [shared_file for shared_file in self.shared_files
                           if shared_file.name == file_name]
                if len(by_name) != 1:
                    files = ", ".join(map(lambda x: x.name, self.shared_files))
                    raise ValueError("Expected 1 shared file, got {}.\nFile: '{}'\nFiles: {}".format(
                        len(by_name), file_name, files))
                artifact = by_name[0]
                assert len(artifact.files) == 1
                file = artifact.files[0]
                self.logger.info("Downloading file {} (artifact={} '{}')"
                                 .format(file.id, artifact.id, artifact.name))

                # TODO: implemented in the genologics package?
                response = self.advanced.get("files/{}/download".format(file.id))
                with open(local_path, 'wb') as fd:
                    for chunk in response.iter_content():
                        fd.write(chunk)

                self.logger.info("Download completed, path='{}'".format(local_path))

                if self.cache:
                    if not os.path.exists(cache_directory):
                        os.mkdir(cache_directory)
                    self.logger.info("Copying artifact to cache directory, {}=>{}".format(local_path, cache_directory))
                    shutil.copy(local_path, cache_directory)

        # Add to this list for cleanup:
        if local_path not in self._local_shared_files:
            self._local_shared_files.append(local_path)

        return open(local_path, mode)

    @lazyprop
    def dilution_scheme(self):
        # TODO: The caller needs to provide the robot
        return DilutionScheme(self.step_input_output_repo, "Hamilton")

    @lazyprop
    def shared_files(self):
        """
        Fetches all share files for the current step
        """
        unique = dict()
        # The API input/output map is rather convoluted, but according to
        # the Clarity developers, this is a valid way to fetch all shared result files:
        for input, output in self.current_step.input_output_maps:
            if output['output-generation-type'] == "PerAllInputs":
                unique.setdefault(output["uri"].id, output["uri"])

        artifacts = self.advanced.lims.get_batch(unique.values())
        return artifacts

    @lazyprop
    def _extended_input_artifacts(self):
        artifacts = []
        for input, output in self.current_step.input_output_maps:
            if output['output-generation-type'] == "PerInput":
                artifacts.append(output['uri'])

        # Batch fetch the details about these:
        artifacts_ex = self.advanced.lims.get_batch(artifacts)
        return artifacts_ex

    @lazyprop
    def _extended_input_containers(self):
        """
        Returns a list with all input containers, where each container has been extended with the attribute
        `artifacts`, containing all artifacts in the container
        """
        containers = {artifact.container.id: artifact.container
                      for artifact in self._extended_input_artifacts}
        ret = []
        for container_res in containers.values():
            artifacts_res = [artifact for artifact in self._extended_input_artifacts
                             if artifact.container.id == container_res.id]
            ret.append(Container.create_from_rest_resource(container_res, artifacts_res))
        return ret

    @lazyprop
    def input_container(self):
        """Returns the input container. If there are more than one, an error is raised"""
        return utils.single(self._extended_input_containers)

    def cleanup(self):
        """Cleans up any downloaded resources. This method will be automatically
        called by the framework and does not need to be called by extensions"""
        # Clean up:
        for path in self._local_shared_files:
            if os.path.exists(path):
                self.logger.info("Local shared file '{}' will be removed to ensure "
                                 "that it won't be uploaded again".format(path))
                # TODO: Handle exception
                os.remove(path)

    def local_shared_xml(self, name):
        """
        Returns a local copy of the xml file as a Python object
        """
        with self.local_shared_file(name, "r") as fs:
            tree = objectify.parse(fs)
            return tree.getroot()

    def output_result_file_by_id(self, id):
        """Returns the output result file by id"""
        resource = [f for f in self.output_result_files if f.id == id][0]
        return ResultFile(resource, self.units)

    @property
    def output_result_files(self):
        for _, output in self.current_step.input_output_maps:
            if output["output-type"] == "ResultFile":
                yield output["uri"]

    def update(self, obj):
        """Add an object that has a commit method to the list of objects to update"""
        self._update_queue.append(obj)

    def commit(self):
        """Commits all objects that have been added via the update method, using batch processing if possible"""
        # TODO: Implement batch processing
        for obj in self._update_queue:
            obj.commit()


class Advanced(object):
    """Provides advanced features, should be avoided in extension scripts"""
    def __init__(self, lims):
        self.lims = lims

    def get(self, endpoint):
        """Executes a GET via the REST interface. One should rather use the lims attribute if possible.
        The endpoint is the part after /api/<version>/ in the API URI.
        """
        url = "{}/api/v2/{}".format(BASEURI, endpoint)
        return requests.get(url, auth=(USERNAME, PASSWORD))

