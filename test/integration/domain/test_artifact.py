import unittest
from clarity_ext.repository import StepRepository
from clarity_ext.clarity import ClaritySession
from clarity_ext.domain import Analyte
import itertools


class TestIntegrationAnalyteRepository(unittest.TestCase):
    """
    Tests that validate if the analyte repository works as expected, giving confidence in
    unit tests that use this class.

    Requires steps in a certain condition.
    """

    def test_placed_samples(self):
        """
        Required setup:
          - Step 24-3643 (TODO: configurable) has placed samples on 96 well plates
          - Input:
            - Plate1: D5
            - Plate2: A5, B7, E12
          - Output:
            - Plate1: B5, D6
            - Plate2: A3, E9
        """
        session = ClaritySession.create("24-3643")
        repo = StepRepository(session)
        pairs = filter(lambda pair: isinstance(pair[0], Analyte) and
                                    isinstance(pair[1], Analyte), repo.all_artifacts())
        # Of all the artifacts, we're only going to examine those that map from analytes
        # to analytes:

        self.assertNotEqual(0, len(pairs))
        self.assertTrue(all([input.sample.id == output.sample.id for input, output in pairs]),
                        "Input and output analytes are not correctly paired")

        def group_analytes(analytes):
            keyfunc = lambda analyte: analyte.container.id
            grouped = itertools.groupby(sorted(analytes, key=keyfunc), key=keyfunc)
            return {key: set(x.well.position.__repr__() for x in value) for key, value in grouped}

        inputs = [inp for inp, outp in pairs]
        actual_inputs = group_analytes(inputs)
        expected_inputs = {
            "27-629": set(["B:7", "E:12", "A:5"]),
            "27-628": set(["D:5"]),
        }
        self.assertEqual(expected_inputs, actual_inputs)

        # TODO: Uses non-constant container names
        outputs = [outp for inp, outp in pairs]
        actual_outputs = group_analytes(outputs)
        expected_outputs = {
            "27-630": set(["B:5", "D:6"]),
            "27-631": set(["A:3", "E:9"]),
        }

        self.assertEqual(expected_outputs, actual_outputs)

