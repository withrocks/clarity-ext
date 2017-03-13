import unittest
from mock import MagicMock
from test.unit.clarity_ext import helpers
from clarity_ext.service import ClarityService, ArtifactService


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

    def test_commit_untouched_artifacts_has_no_effect(self):
        """
        If there have been no updates to UDFs, the artifact service should do nothing on commit
        """
        # Fetch some artifacts through the artifact service:
        repo = MagicMock()
        repo.all_artifacts = helpers.two_containers_artifact_set
        artifact_svc = ArtifactService(repo)
        _, outp = artifact_svc.all_artifacts()[0]
        self.assertIsNotNone(outp.udf_target_conc_ngul, "Unexpected test setup")

        clarity_svc = ClarityService(MagicMock(), MagicMock(), MagicMock())
        clarity_svc.update([outp])
        clarity_svc.step_repository.update_artifacts.assert_not_called()

    def test_commit_touched_artifacts_has_effect(self):
        """
        If we update UDFs of an artifact and then commit in the artifact service,
        the repo update method should be called
        """
        repo = MagicMock()
        repo.all_artifacts = helpers.two_containers_artifact_set
        artifact_svc = ArtifactService(repo)
        _, outp = artifact_svc.all_artifacts()[0]

        # Update through the ClarityService, since all objects uses the same update method:
        outp.udf_target_conc_ngul += 1

        clarity_svc = ClarityService(MagicMock(), MagicMock(), MagicMock())
        clarity_svc.update([outp])
        clarity_svc.step_repository.update_artifacts.assert_called_once()
