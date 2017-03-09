from clarity_ext.domain.artifact import Artifact
from clarity_ext.domain.udf import DomainObjectWithUdfMixin


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

    @property
    def is_pool(self):
        if self.samples is None:
            # TODO: Happens only in a test, fix that...
            return False
        return len(self.samples) > 1


class Sample(DomainObjectWithUdfMixin):

    def __init__(self, sample_id, name, project, udf_map=None):
        """
        :param sample_id: The ID of the sample
        :param name: The name of the sample
        :param project: The project domain object
        :param udf_map: An UdfMapping
        """
        super(Sample, self).__init__(udf_map=udf_map)
        self.id = sample_id
        self.name = name
        self.project = project

    def __repr__(self):
        return "<Sample id={}>".format(self.id)


class Project(DomainObjectWithUdfMixin):
    def __init__(self, name):
        self.name = name

