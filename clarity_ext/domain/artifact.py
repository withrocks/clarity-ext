from clarity_ext.domain import Analyte
from clarity_ext.domain import Container


class StepInputOutputMapRepository(object):
    """
    The REST API provides `Artifact`s. These are wrapped as other domain objects.
    """
    def __init__(self, session):
        self.session = session

    def all_analytes(self):
        """
        Fetches a tuple of (input_analytes, output_analytes). They are in sorted order.
        """
        inputs, outputs = self.all_artifacts()

        def sorted_analytes(seq):
            return sorted(filter(lambda x: isinstance(x, Analyte), seq), key=lambda entry: entry.sample.id)

        input_analytes = sorted_analytes(inputs)
        output_analytes = sorted_analytes(outputs)
        return input_analytes, output_analytes

    def all_artifacts(self):
        """Returns all artifacts in the current step. All have been mapped to domain objects.

        NOTE: For simplicity, this fetches the entire input output map, including
        batch fetching. This will lead to more calls than needed in many cases
        but makes development much simpler. Will be optimized later if needed.
        """
        # TODO: Uses two calls, could use the input output map directly:
        inputs = self.session.current_step.all_inputs(unique=True, resolve=True)
        outputs = self.session.current_step.all_outputs(unique=True, resolve=True)

        inputs = list(self._wrap_artifacts(inputs))
        outputs = list(self._wrap_artifacts(outputs))
        return inputs, outputs

    def _wrap_artifacts(self, artifacts):
        """
        Wraps an artifact in a domain object, if one exists. The domain objects provide logic
        convenient methods for working with the domain object in extensions.
        """
        for artifact in artifacts:
            if artifact.type == "Analyte":
                yield Analyte.create_from_rest_resource(artifact)
            else:
                yield artifact
