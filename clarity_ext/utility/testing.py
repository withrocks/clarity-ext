"""
Various helpers for mocking data quickly, in either unit tests or notebooks.
"""
from clarity_ext.domain import *
from clarity_ext.service.dilution.service import *


class TestDataHelper:
    """
    A helper for creating mock containers and artifacts in as simple a way as possible, even
    for end-users testing things in notebooks, but also in the tests.

    Add to clarity-ext (and not the test module, as it should be usable for anyone)
    """
    def __init__(self, dilution_settings, create_well_order=Container.DOWN_FIRST):
        self.input_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE,
                                         container_id="input", name="input")
        self.output_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE,
                                          container_id="output", name="output")
        self.concentration_unit = dilution_settings.concentration_ref
        self.well_enumerator = self.input_container.enumerate_wells(create_well_order)
        self.pairs = list()

    def _create_analyte(self, is_input, partial_name):
        name = "{}-{}".format("in" if is_input else "out", partial_name)
        ret = Analyte(api_resource=None, is_input=is_input,
                      id=name, name=name)
        return ret

    def create_pair(self, pos_from=None, pos_to=None):
        if pos_from is None:
            well = self.well_enumerator.next()
            pos_from = well.position
        if pos_to is None:
            pos_to = pos_from

        name = "FROM:{}".format(pos_from)
        pair = ArtifactPair(self._create_analyte(True, name),
                            self._create_analyte(False, name))
        self.input_container.set_well(pos_from, artifact=pair.input_artifact)
        self.output_container.set_well(pos_to, artifact=pair.output_artifact)
        self.pairs.append(pair)
        return pair

    def create_dilution_pair(self, conc1, vol1, conc2, vol2, pos_from=None, pos_to=None):
        """Creates an analyte pair ready for dilution"""
        pair = self.create_pair(pos_from, pos_to)
        concentration_unit = DilutionSettings.concentration_unit_to_string(self.concentration_unit)
        conc_source_udf = "Conc. Current ({})".format(concentration_unit)
        conc_target_udf = "Target conc. ({})".format(concentration_unit)
        pair.input_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                  "Current sample volume (ul)": vol1})
        pair.output_artifact.udf_map = UdfMapping({conc_source_udf: conc1,
                                                   "Current sample volume (ul)": vol1,
                                                   "Target vol. (ul)": vol2,
                                                   conc_target_udf: conc2})
        return pair

