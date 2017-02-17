import unittest
from clarity_ext.utility.testing import mock_context
from test.unit.clarity_ext.helpers import mock_two_containers_artifact_service


class TestContext(unittest.TestCase):

    def test_context(self):
        """Can create an ExtensionContext"""
        context = self._mock_context()
        self.assertIsNotNone(context)

    def test_input_output_container_throws(self):
        context = self._mock_context()
        containers = context.artifact_service.all_input_containers()
        self.assertEqual(2, len(containers), "Test data not correctly setup")

        def input_should_raise():
            print context.input_container

        def output_should_raise():
            print context.output_container

        # Fetching a single output_container should throw in this case
        self.assertRaises(ValueError, input_should_raise)
        self.assertRaises(ValueError, output_should_raise)

    def _mock_context(self):
        return mock_context(artifact_service=mock_two_containers_artifact_service())
