from clarity_ext.domain.artifact import Artifact
from clarity_ext.domain.analyte import Analyte
from clarity_ext.domain.result_file import ResultFile
from clarity_ext.domain.shared_result_file import SharedResultFile
from clarity_ext.repository.container_repository import ContainerRepository
from clarity_ext.domain.user import User
from clarity_ext.domain import ProcessType


class StepRepository(object):
    """
    Provides access to data that's available through a current step.

    All methods return the domain objects, wrapping the REST resources.

    Since the underlying library does caching, this repository does not need
    to do that.
    """

    def __init__(self, session, clarity_mapper):
        """
        Creates a new StepRepository

        :param session: A session object for connecting to Clarity
        """
        self.session = session
        self.clarity_mapper = clarity_mapper


    def all_artifacts(self):
        """
        Fetches all artifacts from the input output map, wraps them in to a domain object.
        From then on, the domain object knows the following:
            * From what resource was it built (for debug reasons, e.g. for getting the URL)
            * Is it an input or output artifact
            * What's the corresponding input or output object (input objects have a reference
              to the output object and vice-versa

        After this, all querying should be done on these domain objects.

        The list is not unique, i.e. artifacts will be fetched more than once.

        Performance note: This method may fetch much more data than necessary as it's designed
        for simplified use of the API. If optimal performance is required, use the underlying REST API
        instead.
        """
        input_output_maps = self.session.current_step.api_resource.input_output_maps
        artifact_keys = set()
        for input, output in input_output_maps:
            artifact_keys.add(input["uri"])
            artifact_keys.add(output["uri"])
        artifacts = self.session.api.get_batch(artifact_keys)
        artifacts_by_uri = {artifact.uri: artifact for artifact in artifacts}
        for input, output in input_output_maps:
            input['uri'] = artifacts_by_uri[input['uri'].uri]
            output['uri'] = artifacts_by_uri[output['uri'].uri]

        # Artifacts do not contain UDFs that have not been given a value. Since the domain objects returned
        # must know all UDFs available, we fetch them here:
        # TODO: Move this to the service
        process_type = self.get_process_type()

        ret = []
        # TODO: Ensure that the container repo fetches all containers in one
        # batch call:
        container_repo = ContainerRepository()
        for input_res, output_res in input_output_maps:
            input, output = self._wrap_input_output(
                input_res, output_res, container_repo, process_type)
            ret.append((input, output))
        return ret

    def _wrap_input_output(self, input_info, output_info, container_repo, process_type):

        # Create a map of all containers, so we can fill in it while building
        # domain objects.

        # Create a fresh container repository. Then we know that only one container
        # will be created for each object in a call to this method
        input_resource = input_info["uri"]
        output_resource = output_info["uri"]
        output_gen_type = output_info["output-generation-type"]
        input = self._wrap_artifact(
            input_resource, container_repo, gen_type="Input", is_input=True, process_type=process_type)
        output = self._wrap_artifact(output_resource, container_repo,
                                     gen_type=output_gen_type, is_input=False, process_type=process_type)

        if output_gen_type == "PerInput":
            output.generation_type = Artifact.PER_INPUT
        elif output_gen_type == "PerAllInputs":
            output.generation_type = Artifact.PER_ALL_INPUTS
        else:
            raise NotImplementedError(
                "Generation type {} is not implemented".format(output_gen_type))

        if isinstance(output, ResultFile):
            output.output_type = Artifact.OUTPUT_TYPE_RESULT_FILE
        elif isinstance(output, Analyte):
            output.output_type = Artifact.OUTPUT_TYPE_ANALYTE
        elif isinstance(output, SharedResultFile):
            output.output_type = Artifact.OUTPUT_TYPE_SHARED_RESULT_FILE
        else:
            raise NotImplementedError(
                "Output type {} is not implemented".format(output.__class__.__name__))

        # Add a reference to the other object for convenience:
        input.output = output
        output.input = input

        return input, output

    def _wrap_artifact(self, artifact, container_repo, gen_type, is_input, process_type):
        """
        Wraps an artifact in a domain object, if one exists. The domain objects provide logic
        convenient methods for working with the domain object in extensions.
        """
        if artifact.type == "Analyte":
            wrapped = self.clarity_mapper.analyte_create_object(
                artifact, is_input, container_repo, process_type)
        elif artifact.type == "ResultFile" and gen_type == "PerInput":
            wrapped = self.clarity_mapper.result_file_create_object(
                artifact, is_input, container_repo, process_type)
        elif artifact.type == "ResultFile" and gen_type == "PerAllInputs":
            wrapped = SharedResultFile.create_from_rest_resource(artifact, process_type)
        else:
            raise Exception("Unknown type and gen_type combination {}, {}".format(
                artifact.type, gen_type))

        # TODO: This is kind of a hack for adding the parent process. Make more use of OOP.
        try:
            wrapped.parent_process = artifact.parent_process
        except AttributeError:
            wrapped.parent_process = None

        return wrapped

    def _wrap_artifacts(self, artifacts):
        for artifact in artifacts:
            yield self._wrap_artifact(artifact)

    def update_artifacts(self, artifacts):
        """Updates all the artifact resources"""
        self.session.api.put_batch(artifacts)

    def current_user(self):
        current_user_resource = self.session.current_step.technician
        return User.create_from_rest_resource(current_user_resource)

    def get_process_type(self):
        """Returns the process type of the current process"""
        self.session.current_step.api_resource.type.get()
        return ProcessType.create_from_resource(self.session.current_step.api_resource.type)

