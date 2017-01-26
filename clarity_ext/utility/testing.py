"""
Various helpers for mocking data quickly, in either unit tests or notebooks.
"""
from clarity_ext.domain import *


class TestDataHelper:
    """
    A helper for creating mock containers and artifacts in as simple a way as possible, even
    for end-users testing things in notebooks, but also in the tests.

    Add to clarity-ext (and not the test module, as it should be usable for anyone)
    """
    def __init__(self):
        self.input_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE)
        self.output_container = Container(container_type=Container.CONTAINER_TYPE_96_WELLS_PLATE)
        self.pairs = list()

    def _create_analyte(self, is_input, partial_name):
        name = "{}-{}".format("in" if is_input else "out", partial_name)
        ret = Analyte(api_resource=None, is_input=is_input,
                      id=name, name=name)
        return ret

    def create_pair(self, name, location=None):
        pair = ArtifactPair(self._create_analyte(True, name),
                            self._create_analyte(False, name))
        # TODO: Add a method on the container where you can push to the next
        # location in a certain direction:
        self.input_container.set_well("A:1", artifact=pair.input_artifact)
        self.output_container.set_well("A:1", artifact=pair.output_artifact)
        self.pairs.append(pair)
        return pair

    def create_dilution_pair(self,
                             name,
                             current_concentration,
                             current_volume,
                             target_concentration,
                             target_volume,
                             location=None):
        """Creates an analyte pair ready for dilution"""
        pair = self.create_pair(name, location)
        pair.input_artifact.udf_map = UdfMapping({"Conc. Current (nM)": current_concentration,
                                                  "Current sample volume (ul)": current_volume})
        pair.output_artifact.udf_map = UdfMapping({"Conc. Current (nM)": current_concentration,
                                                   "Current sample volume (ul)": current_volume,
                                                   "Target vol. (ul)": target_volume,
                                                   "Target conc. (nM)": target_concentration})
        return pair


