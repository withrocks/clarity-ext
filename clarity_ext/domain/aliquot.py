from clarity_ext.domain.artifact import Artifact
from clarity_ext.utils import get_and_apply
from clarity_ext.domain.common import DomainObjectMixin


class Aliquot(Artifact):

    def __init__(self, api_resource, is_input, id=None, sample=None, name=None, well=None,
                 artifact_specific_udf_map=None, **kwargs):
        super(Aliquot, self).__init__(
            api_resource=api_resource, id=id, name=name,
            artifact_specific_udf_map=artifact_specific_udf_map)
        self.sample = sample
        self.well = well
        self.is_input = is_input
        if well:
            self.container = well.container
        else:
            self.container = None
        self.concentration = get_and_apply(
            kwargs, "concentration", None, float)
        self.volume = get_and_apply(kwargs, "volume", None, float)


class Sample(DomainObjectMixin):
    def __init__(self, sample_id):
        self.id = sample_id

    def __repr__(self):
        return "<Sample id={}>".format(self.id)

    @staticmethod
    def create_from_rest_resource(resource):
        sample = Sample(resource.id)
        return sample
