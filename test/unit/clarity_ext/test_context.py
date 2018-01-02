import unittest
from test.unit.clarity_ext.helpers import mock_context


class TestContext(unittest.TestCase):

    def test_context(self):
        """Can create an ExtensionContext"""
        context = mock_context()
        self.assertIsNotNone(context)

    def test_input_output_container_throws(self):
        context = mock_context()

        def input_should_raise():
            print(context.input_container)

        def output_should_raise():
            print(context.output_container)

        # Fetching a single output_container should throw in this case
        self.assertRaises(ValueError, input_should_raise)
        self.assertRaises(ValueError, output_should_raise)

    def _mock_context(self):
        return mock_context(artifact_service=mock_two_containers_artifact_service())
