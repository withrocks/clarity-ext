from clarity_ext.domain.artifact import Artifact
from clarity_ext.domain.udf import DomainObjectMixin, DomainObjectWithUdfMixin
from clarity_ext.domain.container import ContainerPosition


class Aliquot(Artifact):
    """
    NOTE: This class currently acts as a base class for both Analyte and ResultFile. It will be
    merged with Analyte, since the name can cause some confusion as Analytes
    and ResultFiles strictly don't need to be Aliquots, i.e. they can be non-divided copies
    of the original for example. Or, in the case of ResultFile, only a measurement of the original.
    """

    def __init__(self, api_resource, is_input, id=None, samples=None, name=None, well=None, udf_map=None):
        super(Aliquot, self).__init__(api_resource=api_resource, id=id, name=name, udf_map=udf_map)
        self.samples = samples
        self.well = well
        self.is_input = is_input
        if well:
            self.container = well.container
            well.artifact = self
        else:
            self.container = None
        self.is_from_original = False

    @staticmethod
    def create_well_from_rest(resource, container_repo, is_input):
        # TODO: Batch call
        try:
            container = container_repo.get_container(resource.location[0], is_input)
        except AttributeError:
            pass
            container = None
        try:
            pos = ContainerPosition.create(resource.location[1])
        except (AttributeError, ValueError):
            pass
            pos = None

        well = None
        if container and pos:
            well = container.wells[pos]

        return well


class Sample(DomainObjectWithUdfMixin):

    def __init__(self, sample_id, name, project, udfs=None):
        """
        :param sample_id: The ID of the sample
        :param name: The name of the sample
        :param project: The project domain object
        :param udfs: A dictionary of udfs
        """
        super(Sample, self).__init__(udfs)
        self.id = sample_id
        self.name = name
        self.project = project

    def __repr__(self):
        return "<Sample id={}>".format(self.id)

    @staticmethod
    def create_from_rest_resource(resource):
        project = Project(resource.project.name) if resource.project else None
        sample = Sample(resource.id, resource.name, project, resource.udf)
        return sample


class Project(DomainObjectWithUdfMixin):
    def __init__(self, name):
        self.name = name

