import logging
from clarity_ext.domain.validation import ValidationType, UsageError


class ValidationService:

    def __init__(self, step_logger_service, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.step_logger_service = step_logger_service
        self.warning_count = 0
        self.error_count = 0

    def handle_validation(self, results):
        """
        Pushes validation results to the logging framework
        """
        if len(results) > 0:
            self._log_debug("Validation errors, len = {}".format(len(results)))
            for result in results:
                self._log_debug(result)
                self.handle_single_validation(result)
        # If any of the validation results were errors, raise an exception:
        if any(result for result in results if result.type == ValidationType.ERROR):
            raise UsageError("Errors during validation. See the step log for further details.", results)

    def handle_single_validation(self, result):
        msg_row = "{}".format(result)
        self.step_logger_service.log(msg_row)
        self._log_debug("{}".format(msg_row))

        if result.type == ValidationType.ERROR:
            self.error_count += 1
        if result.type == ValidationType.WARNING:
            self.warning_count += 1

    def _log_debug(self, msg):
        if self.logger is not None:
            self.logger.debug(msg)
