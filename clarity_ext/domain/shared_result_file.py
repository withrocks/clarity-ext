from clarity_ext.domain.artifact import Artifact
from clarity_ext.domain.udf import UdfMapping
from clarity_ext import utils


class SharedResultFile(Artifact):
    """
    TODO: Document
    """

    def __init__(self, api_resource=None, id=None, name=None, udf_map=None, files=None):
        super(SharedResultFile, self).__init__(api_resource=api_resource,
                                               id=id,
                                               name=name,
                                               udf_map=udf_map)
        self.files = files or list()

    @staticmethod
    def create_from_rest_resource(resource, process_type=None):
        name = resource.name
        process_output = utils.single([process_output for process_output in process_type.process_outputs
                                       if process_output.output_generation_type == "PerAllInputs" and
                                       process_output.artifact_type == "ResultFile"])
        udfs = UdfMapping.expand_udfs(resource, process_output)
        udf_map = UdfMapping(udfs)

        # TODO: fix the files
        return SharedResultFile(api_resource=resource, id=resource.id, name=name, udf_map=udf_map, files=resource.files)

    def __repr__(self):
        typename = type(self).__name__
        return "{}<{} ({})>".format(typename, self.name, self.id)


class SharedResultFileAttachment(object):
    pass