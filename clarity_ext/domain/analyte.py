from clarity_ext.utils import get_and_apply
from clarity_ext.domain.aliquot import Aliquot, Sample
from clarity_ext import utils


class Analyte(Aliquot):
    """
    Describes an Analyte in the Clarity LIMS system.

    Expects certain mappings to UDFs in clarity. These are provided
    in udf_map, so they can be overridden in different installations.
    """

    def __init__(self, api_resource, is_input, id=None, samples=None, name=None, well=None,
                 artifact_specific_udf_map=None, **kwargs):
        """
        Creates an analyte
        """
        super(self.__class__, self).__init__(api_resource, is_input=is_input, id=id,
                                             samples=samples, name=name, well=well,
                                             artifact_specific_udf_map=artifact_specific_udf_map, **kwargs)
        self.target_concentration = get_and_apply(
            kwargs, "target_concentration", None, float)
        self.target_volume = get_and_apply(
            kwargs, "target_volume", None, float)

    def __repr__(self):
        return "{} ({})".format(self.name, self.id)

    @staticmethod
    def create_from_rest_resource(resource, is_input, udf_map, container_repo):
        """
        Creates an Analyte from the rest resource. By default, the container
        is created from the related container resource, except if one
        already exists in the container map. This way, there will be created
        only one container object for each id
        """

        # Map UDFs (which may be using different names in different Clarity setups)
        # to a key-value list with well-defined key names:
        analyte_udf_map = udf_map.get("Analyte", None)
        kwargs = {key: resource.udf.get(
            analyte_udf_map[key], None) for key in analyte_udf_map}
        well = Aliquot.create_well_from_rest(
            resource=resource, container_repo=container_repo)

        # TODO: sample should be put in a lazy property, and all samples in a step should be
        # loaded in one batch
        samples = [Sample.create_from_rest_resource(sample) for sample in resource.samples]
        analyte = Analyte(api_resource=resource, is_input=is_input, id=resource.id,
                          samples=samples, name=resource.name,
                          well=well, artifact_specific_udf_map=analyte_udf_map, **kwargs)
        analyte.api_resource = resource

        return analyte

    def updated_rest_resource(self, original_rest_resource, updated_fields):
        """
        :param original_rest_resource: The rest resource in the state as in the api cache
        :return: An updated rest resource according to changes in this instance of Analyte
        """

        _updated_rest_resource = \
            super(self.__class__, self).updated_rest_resource(original_rest_resource, updated_fields)

        # Add analyte specific fields here ...
        if 'name' in updated_fields:
            _updated_rest_resource.name = self.assigner.register_assign('name', self.name)
        return _updated_rest_resource, self.assigner.consume()

    @property
    def sample(self):
        """
        Returns a single sample for convenience. Throws an error if there isn't exactly one sample.

        NOTE: There can be more than one sample on an Analyte. That's the case with pools.
        """
        return utils.single(self.samples)

