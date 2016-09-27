from clarity_ext.domain.artifact import Artifact


class SharedResultFile(Artifact):
    def __init__(self, api_resource=None, id=None, name=None, artifact_specific_udf_map=None):
        super(SharedResultFile, self).__init__(
            api_resource=api_resource, id=id, name=name,
            artifact_specific_udf_map=artifact_specific_udf_map)

    @staticmethod
    def create_from_rest_resource(api_resource, udf_map=None):
        shared_result_file_udf_map = udf_map.get("SharedResultFile")
        name = api_resource.name
        return SharedResultFile(api_resource=api_resource, id=api_resource.id, name=name,
                                artifact_specific_udf_map=shared_result_file_udf_map)
