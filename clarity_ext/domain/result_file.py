from clarity_ext.unit_conversion import UnitConversion
from clarity_ext.domain.artifact import Artifact


class ResultFile(Artifact):
    """Encapsulates a ResultFile in Clarity"""

    def __init__(self, api_resource, units, id=None):
        super(self.__class__, self).__init__()
        self.api_resource = api_resource
        self.units = units
        self.id = id

    def commit(self):
        self.api_resource.put()

    def set_udf(self, name, value, from_unit=None, to_unit=None):
        if from_unit:
            value = self.units.convert(value, from_unit, to_unit)
        self.api_resource.udf[name] = value

    @staticmethod
    def create_from_rest_resource(resource, container_repo):
        """
        Creates a `ResultFile` from the REST resource object.
        The container is fetched from the container_repo.
        """
        ret = ResultFile(api_resource=resource,
                         units=UnitConversion(), id=resource.id)
        ret._resource = resource

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

    def __repr__(self):
        return self.id
