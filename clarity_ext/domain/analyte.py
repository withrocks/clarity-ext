from clarity_ext.domain.aliquot import Aliquot, Sample
from clarity_ext import utils
from clarity_ext.domain.udf import UdfMapping


class Analyte(Aliquot):
    """
    Describes an Analyte in the Clarity LIMS system.

    Expects certain mappings to UDFs in clarity. These are provided
    in udf_map, so they can be overridden in different installations.
    """

    def __init__(self, api_resource, is_input, id=None, samples=None, name=None, well=None,
                 is_control=False, udf_map=None, is_from_original=None):
        """
        Creates an analyte
        """
        super(self.__class__, self).__init__(api_resource, is_input=is_input, id=id,
                                             samples=samples, name=name, well=well,
                                             udf_map=udf_map)
        self.is_control = is_control
        self.is_output_from_previous = is_from_original

    def __repr__(self):
        typename = type(self).__name__
        if self.is_input is not None:
            typename = ("Input" if self.is_input else "Output") + typename
        return "{}<{} ({})>".format(typename, self.name, self.id)

    @staticmethod
    def create_from_rest_resource(resource, is_input, container_repo, process_type):
        """
        Creates an Analyte from the rest resource. By default, the container
        is created from the related container resource, except if one
        already exists in the container map. This way, there will be created
        only one container object for each id
        """
        # Map UDFs (which may be using different names in different Clarity setups)
        # to a key-value list with well-defined key names:

        udfs = None
        if not is_input:
            per_input_analytes = [process_output for process_output
                                  in process_type.process_outputs
                                  if process_output.output_generation_type == "PerInput" and
                                  process_output.artifact_type == "Analyte"]
            process_output = utils.single_or_default(per_input_analytes)
            if process_output:
                udfs = UdfMapping.expand_udfs(resource, process_output)

        if udfs is None:
            udfs = resource.udf

        udf_map = UdfMapping(udfs)

        well = Aliquot.create_well_from_rest(
            resource=resource, container_repo=container_repo)

        # TODO: sample should be put in a lazy property, and all samples in a step should be
        # loaded in one batch
        samples = [Sample.create_from_rest_resource(sample) for sample in resource.samples]

        is_control = False
        # TODO: This code principally belongs to the genologics layer, but 'control-type' does not exist there
        if resource.root.find("control-type") is not None:
            is_control = True
        # TODO: A better way to decide if analyte is output of a previous step?
        is_from_original = (resource.id.find("2-") != 0)
        analyte = Analyte(api_resource=resource, is_input=is_input, id=resource.id,
                          samples=samples, name=resource.name,
                          well=well, is_control=is_control,
                          udf_map=udf_map,
                          is_from_original=is_from_original)
        analyte.api_resource = resource
        analyte.reagent_labels = resource.reagent_labels

        return analyte

    @property
    def sample(self):
        """
        Returns a single sample for convenience. Throws an error if there isn't exactly one sample.

        NOTE: There can be more than one sample on an Analyte. That's the case with pools.
        """
        return utils.single(self.samples)
