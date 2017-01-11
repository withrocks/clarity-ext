import unittest
from mock import MagicMock
from mock import call
import logging
from clarity_ext.extensions import ValidationService
from clarity_ext.domain.validation import ValidationException
from clarity_ext.domain.validation import ValidationType
from clarity_ext.context import ExtensionContext


class TestValidationService(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__module__)

    def instantiate_context(self):
        session = MagicMock()
        artifact_svc = MagicMock()
        file_svc = MagicMock()
        current_user = MagicMock()
        step_logger_svc = MagicMock()
        clarity_svc = MagicMock()
        return ExtensionContext(session, artifact_svc, file_svc, current_user,
                                step_logger_svc, None, clarity_svc, dilution_service=MagicMock())

    def test_validation_logging(self):
        context = self.instantiate_context()
        validation_service = ValidationService(
            context=context, logger=self.logger)
        step_logger_service = MagicMock()
        step_logger_service.log = MagicMock()
        validation_service.step_logger_service = step_logger_service
        error_exception = ValidationException(
            "Error message", validation_type=ValidationType.ERROR)
        warning_exception = ValidationException(
            "Warning message", validation_type=ValidationType.WARNING)
        validation_service.handle_validation([error_exception])

        validation_service.step_logger_service.log.assert_called_once_with(
            "{}".format(error_exception))
        self.assertEqual(validation_service.has_errors, True)
        self.assertEqual(validation_service.has_warnings, False)

        validation_service.handle_validation(
                [error_exception, warning_exception])
        calls = [call("{}".format(error_exception)),
                 call("{}".format(warning_exception))]
        validation_service.step_logger_service.log.assert_has_calls(
            calls, any_order=False)
        self.assertEqual(validation_service.has_errors, True)
        self.assertEqual(validation_service.has_warnings, True)
