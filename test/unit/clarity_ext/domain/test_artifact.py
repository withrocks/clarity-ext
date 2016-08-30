import unittest
from clarity_ext.domain import Artifact


class TestArtifact(unittest.TestCase):

    def test_two_identical_artifacts_equal(self):
        """A copy of an artifact should be equal to another"""
        artifacts = [Artifact(), Artifact()]
        for artifact in artifacts:
            artifact.generation_type = Artifact.OUTPUT_TYPE_RESULT_FILE
            artifact.is_input = False
            artifact.any_patch = "13370"

        self.assertEqual(artifacts[0], artifacts[1])

    def test_artifact_should_not_equal_non_artifact(self):
        artifact = Artifact()
        self.assertNotEqual(artifact, "string")
