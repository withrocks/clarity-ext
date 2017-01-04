import unittest
from clarity_ext import dilution
from clarity_ext import ExtensionContext
import requests_cache


class TestDilutionService(unittest.TestCase):
    def test_dilution_service(self):
        requests_cache.configure("tempo")

        # A temporary tdd test for refactoring of the dilution service
        context = ExtensionContext.create("24-13693")
        context.dilution_service.validate_can_execute_in_context()


        # This will dilute using the artifacts from the artifact service. It would make sense to instead send
        # in the artifacts that should be diluted, as that makes testing with mocks much simpler
        # It's also very unclear that this actually dilutes!
        pairs = context.artifact_service.all_aliquot_pairs()
        dilution_scheme = context.dilution_service.init_dilution_scheme(pairs,
                                                                        concentration_ref=dilution.CONCENTRATION_REF_NGUL,
                                                                        volume_calc_method=dilution.VOLUME_CALC_BY_CONC)
        hamilton_driver_file = \
            context.dilution_service.create_robot_driver_file(dilution_scheme,
                                                              context.dilution_service.ROBOT_HAMILTON,
                                                              pairs[0].output_artifact.container.size)

        for line in hamilton_driver_file:
            print(line.strip())
