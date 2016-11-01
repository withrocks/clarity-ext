from clarity_ext.domain.udf import Udf
from clarity_ext.domain.common import DomainObjectMixin


class Artifact(Udf):
    """
    Represents any input or output artifact from the Clarity server, e.g. an Analyte
    or a ResultFile.
    """
    PER_INPUT = 1
    PER_ALL_INPUTS = 2

    OUTPUT_TYPE_RESULT_FILE = 1
    OUTPUT_TYPE_ANALYTE = 2
    OUTPUT_TYPE_SHARED_RESULT_FILE = 3

    def __init__(self, api_resource=None, id=None, name=None, artifact_specific_udf_map=None):
        super(Artifact, self).__init__(api_resource=api_resource, id=id,
                                       entity_specific_udf_map=artifact_specific_udf_map)
        self.is_input = None  # Set to true if this is an input artifact
        self.generation_type = None  # Set to PER_INPUT or PER_ALL_INPUTS if applicable
        self.name = name


class ArtifactPair(DomainObjectMixin):
    """
    Represents an input/output pair of artifacts
    """

    def __init__(self, input_artifact, output_artifact):
        self.input_artifact = input_artifact
        self.output_artifact = output_artifact

    def __repr__(self):
        return "{}, {}".format(self.input_artifact.id, self.output_artifact.id)

