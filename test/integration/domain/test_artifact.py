import unittest
from clarity_ext.domain.artifact import StepRepository
from clarity_ext.clarity import ClaritySession
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
        inputs, outputs = repo.all_analytes()
        self.assertEqual(len(outputs), len(inputs), "Expected same number of inputs and outputs")
        self.assertEqual([input.sample.id for input in inputs],
                         [output.sample.id for output in outputs],
                         "Expected inputs and outputs to be in the same order")

        def group_analytes(analytes):
            keyfunc = lambda analyte: analyte.container.id
            grouped = itertools.groupby(sorted(analytes, key=keyfunc), key=keyfunc)
            return {key: set(x.well.position.__repr__() for x in value) for key, value in grouped}

        actual_inputs = group_analytes(inputs)
        expected_inputs = {
            "27-629": set(["B:7", "E:12", "A:5"]),
            "27-628": set(["D:5"]),
        }
        self.assertEqual(expected_inputs, actual_inputs)

        # TODO: Uses non-constant container names
        actual_outputs = group_analytes(outputs)
        expected_outputs = {
            "27-630": set(["B:5", "D:6"]),
            "27-631": set(["A:3", "E:9"]),
        }

        self.assertEqual(expected_outputs, actual_outputs)

