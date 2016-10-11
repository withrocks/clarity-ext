import unittest
from clarity_ext.context import ExtensionContext
from mock import MagicMock
from test.unit.clarity_ext import helpers


class TestContext(unittest.TestCase):

    def test_context(self):
        """Can create an ExtensionContext"""
        session = MagicMock()
        artifact_svc = MagicMock()
        file_svc = MagicMock()
        current_user = MagicMock()
        step_logger_svc = MagicMock()
        context = ExtensionContext(session, artifact_svc, file_svc, current_user, step_logger_svc, None)
        self.assertIsNotNone(context)

    def test_input_output_container_throws(self):
        session = MagicMock()
        artifact_svc = helpers.mock_two_containers_artifact_service()
        file_svc = MagicMock()
        current_user = MagicMock()
        step_logger_svc= MagicMock()
        context = ExtensionContext(session, artifact_svc, file_svc, current_user, step_logger_svc, None)

        containers = artifact_svc.all_input_containers()
        self.assertEqual(2, len(containers), "Test data not correctly setup")

        def input_should_raise():
            print context.input_container

        def output_should_raise():
            print context.output_container

        # Fetching a single output_container should throw in this case
        self.assertRaises(ValueError, input_should_raise)
        self.assertRaises(ValueError, output_should_raise)
