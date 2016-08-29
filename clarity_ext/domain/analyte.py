from clarity_ext.domain.container import PlatePosition, Well, Container
from clarity_ext.utils import get_and_apply


class Analyte(Artifact):
    """
    Describes an Analyte in the Clarity LIMS system.

    Expects certain mappings to UDFs in clarity. These are provided
    in udf_map, so they can be overridden in different installations.
    """

    def __init__(self, container, name=None, well=None, sample=None, **kwargs):
        """
        Creates an analyte
        """
        self.container = container
        self.name = name
        self.well = well
        self.sample = sample

        self.concentration = get_and_apply(kwargs, "concentration", None, float)
        self.target_concentration = get_and_apply(kwargs, "target_concentration", None, float)
        self.target_volume = get_and_apply(kwargs, "target_volume", None, float)

    def __repr__(self):
        return "{} ({})".format(self.name, self.sample.id)

    @staticmethod
    def create_from_rest_resource(resource, udf_map):
        container = Container.create_from_rest_resource(resource.location[0], [])

        # Map UDFs (which may be using different names in different Clarity setups)
        # to a key-value list with well-defined key names:
        analyte_udf_map = udf_map["Analyte"]
        kwargs = {key: resource.udf.get(analyte_udf_map[key], None) for key in analyte_udf_map}
        pos = PlatePosition.create(resource.location[1])
        well = Well(pos, container)
        sample = Sample.create_from_rest_resource(resource.samples[0])
        return Analyte(container, resource.name, well, sample, **kwargs)


class Sample(DomainObjectMixin):
    def __init__(self, sample_id):
        self.id = sample_id

    def __repr__(self):
        return "<Sample id={}>".format(self.id)

    @staticmethod
    def create_from_rest_resource(resource):
        sample = Sample(resource.id)
        return sample

