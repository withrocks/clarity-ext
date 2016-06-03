from clarity_ext.domain.container import PlatePosition, Well, Container


class Analyte(object):
    """
    Describes an Analyte in the Clarity LIMS system, including custom UDFs.

    Takes an analyte resource as input.
    """

    def __init__(self, resource, container):
        """
        :resource: The API resource
        """
        self.resource = resource
        self.container = container

    # Mapped properties from the underlying resource
    @property
    def name(self):
        return self.resource.name

    # Mapped UDFs
    @property
    def concentration(self):
        return self.resource.udf["Concentration"]

    @property
    def target_concentration(self):
        return float(self.resource.udf["Target Concentration"])

    @property
    def target_volume(self):
        return self.resource.udf["Target Volume"]

    # Other properties
    @property
    def well(self):
        pos = PlatePosition.create(self.resource.location[1])
        return Well(pos, self.container)

    @property
    def sample(self):
        return self.resource.samples[0]

    def __repr__(self):
        return "{} ({})".format(self.name, self.sample.id)

    @staticmethod
    def create_from_rest_resource(artifact):
        container = Container.create_from_rest_resource(artifact.location[0], [])
        return Analyte(artifact, container)

