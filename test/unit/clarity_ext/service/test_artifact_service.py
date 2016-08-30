import unittest
from test.unit.clarity_ext import helpers


class TestArtifactService(unittest.TestCase):

    def test_output_containers_expected(self):
        svc = helpers.mock_two_containers_artifact_service()

        # We expect exacty these two containers:
        expected = set(["cont-id3", "cont-id4"])
        actual = set([x.id for x in svc.all_output_containers()])
        print actual
        self.assertEqual(expected, actual)

    def test_input_containers_expected(self):
        svc = helpers.mock_two_containers_artifact_service()

        # We expect exacty these two containers:
        expected = set(["cont-id1", "cont-id2"])
        actual = set([x.id for x in svc.all_input_containers()])
        self.assertEqual(expected, actual)

    def test_input_output_are_in_correct_order(self):
        # Ensures that the service returns tuples of input, output pairs in
        # order
        svc = helpers.mock_two_containers_artifact_service()

        analytes = svc.all_analyte_pairs()

        # Expecting AnalytePair objects, correctly mapped:
        self.assertTrue(all(pair.input_artifact.is_input and
                            not pair.output_artifact.is_input for pair in analytes))
