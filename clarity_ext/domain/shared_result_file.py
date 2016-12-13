from clarity_ext.domain.artifact import Artifact
from clarity_ext.utils import get_and_apply


class SharedResultFile(Artifact):

    def __init__(self, api_resource=None, id=None, name=None, artifact_specific_udf_map=None,
                 **kwargs):
        super(SharedResultFile, self).__init__(
            api_resource=api_resource, id=id, name=name,
            artifact_specific_udf_map=artifact_specific_udf_map)
        self.has_errors = bool(get_and_apply(kwargs, "has_errors", 0, int))

    @staticmethod
    def create_from_rest_resource(api_resource, udf_map=None):
        shared_result_file_udf_map = udf_map.get("SharedResultFile", dict())
        name = api_resource.name
        specific_udf_map = udf_map["SharedResultFile"]
        kwargs = {key: api_resource.udf.get(
            specific_udf_map[key], None) for key in specific_udf_map}

        return SharedResultFile(api_resource=api_resource, id=api_resource.id, name=name,
                                artifact_specific_udf_map=shared_result_file_udf_map, **kwargs)

    def updated_rest_resource(self, original_rest_resource, updated_fields):
        _updated_rest_resource = \
            super(self.__class__, self).updated_rest_resource(
                original_rest_resource, updated_fields)

        return _updated_rest_resource, self.assigner.consume()

    def __repr__(self):
        typename = type(self).__name__
        return "{}<{} ({})>".format(typename, self.name, self.id)
