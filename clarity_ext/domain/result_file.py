from clarity_ext.domain.aliquot import Aliquot, Sample
from clarity_ext import utils


class ResultFile(Aliquot):
    """Encapsulates a ResultFile in Clarity"""

    def __init__(self, api_resource, is_input, id=None, samples=None, name=None, well=None,
                 artifact_specific_udf_map=None, **kwargs):
        super(self.__class__, self).__init__(
            api_resource, is_input=is_input, id=id, samples=samples, name=name, well=well,
            artifact_specific_udf_map=artifact_specific_udf_map, **kwargs)

    @staticmethod
    def create_from_rest_resource(resource, is_input, udf_map, container_repo):
        """
        Creates a `ResultFile` from the REST resource object.
        The container is fetched from the container_repo.
        """

        result_file_udf_map = udf_map.get('ResultFile', None)
        kwargs = {key: resource.udf.get(result_file_udf_map[key], None)
                  for key in result_file_udf_map}

        well = Aliquot.create_well_from_rest(
            resource=resource, container_repo=container_repo)

        # TODO: sample should be put in a lazy property, and all samples in a step should be
        # loaded in one batch
        samples = [Sample.create_from_rest_resource(sample) for sample in resource.samples]
        ret = ResultFile(api_resource=resource, is_input=is_input,
                         id=resource.id, samples=samples, name=resource.name, well=well,
                         artifact_specific_udf_map=result_file_udf_map, **kwargs)

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

    @property
    def sample(self):
        """Convenience property for fetching a single sample when only one is expected"""
        return utils.single(self.samples)

    def __repr__(self):
        typename = type(self).__name__
        return "{}<{} ({})>".format(typename, self.name, self.id)
