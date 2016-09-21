from clarity_ext.unit_conversion import UnitConversion
from clarity_ext.domain.artifact import Artifact


class ResultFile(Artifact):
    """Encapsulates a ResultFile in Clarity"""

    def __init__(self, api_resource, artifact_specific_udf_map, id=None):
        super(self.__class__, self).__init__(api_resource, artifact_specific_udf_map)
        self.id = id

    @staticmethod
    def create_from_rest_resource(resource, udf_map, container_repo):
        """
        Creates a `ResultFile` from the REST resource object.
        The container is fetched from the container_repo.
        """

        result_file_udf_map = udf_map.get('ResultFile', None)
        ret = ResultFile(api_resource=resource,
                         artifact_specific_udf_map=result_file_udf_map, id=resource.id)

        try:
            container_resource = resource.location[0]
            ret.container = container_repo.get_container(container_resource)
            well = resource.location[1]
            ret.container.set_well(well, artifact=ret)
        except AttributeError:
            pass
            ret.container = None

        ret.name = resource.name

        return ret

    def updated_rest_resource(self, original_rest_resource, updated_fields):
        """
        :param original_rest_resource: The rest resource in the state as in the api cache
        :return: An updated rest resource according to changes in this instance of Analyte
        """

        _updated_rest_resource = \
            super(self.__class__, self).updated_rest_resource(original_rest_resource, updated_fields)

        # Add ResultFile specific fields here ...

        return _updated_rest_resource, self.assigner.consume()


    def __repr__(self):
        return self.id
