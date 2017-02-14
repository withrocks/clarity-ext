from clarity_ext.domain.aliquot import Aliquot, Sample
from clarity_ext import utils
from clarity_ext.domain.udf import UdfMapping


class ResultFile(Aliquot):
    """Encapsulates a ResultFile in Clarity"""

    def __init__(self, api_resource, is_input, id=None, samples=None, name=None, well=None,
                 udf_map=None):
        """
        :param api_resource: The original API resource
        :param is_input: True if this is an input analyte, false if not
        :param samples:
        :param name: Name of the result file
        :param well: Well (location, TODO rename) of the result file
        :param udf_map: A list of UdfMappingInfo objects 
        """
        # TODO: Get rid of the api_resource
        super(self.__class__, self).__init__(api_resource, is_input=is_input, id=id,
                samples=samples, name=name, well=well, udf_map=udf_map)
        self.is_control = False

    @staticmethod
    def create_from_rest_resource(resource, is_input, container_repo, process_type):
        """
        Creates a `ResultFile` from the REST resource object.
        The container is fetched from the container_repo.
        """
        if not is_input:
            # We expect the process_type to define one PerInput ResultFile
            process_output = utils.single([process_output for process_output in process_type.process_outputs
                                           if process_output.output_generation_type == "PerInput" and
                                           process_output.artifact_type == "ResultFile"])
        udfs = UdfMapping.expand_udfs(resource, process_output)
        udf_map = UdfMapping(udfs)

        well = Aliquot.create_well_from_rest(
            resource=resource, container_repo=container_repo)

        # TODO: sample should be put in a lazy property, and all samples in a step should be
        # loaded in one batch
        samples = [Sample.create_from_rest_resource(sample) for sample in resource.samples]
        ret = ResultFile(api_resource=resource, is_input=is_input,
                         id=resource.id, samples=samples, name=resource.name, well=well,
                         udf_map=udf_map)
        return ret

    @property
    def sample(self):
        """Convenience property for fetching a single sample when only one is expected"""
        return utils.single(self.samples)

    def __repr__(self):
        typename = type(self).__name__
        return "{}<{} ({})>".format(typename, self.name, self.id)
