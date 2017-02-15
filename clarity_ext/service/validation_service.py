from clarity_ext.service.step_logger_service import StepLoggerService
from clarity_ext.domain.validation import ValidationType


class ValidationService:

    def __init__(self, step_logger_service, logger=None):
        self.logger = logger
        self.step_logger_service = step_logger_service

    def handle_validation(self, results):
        """
        Pushes validation results to the logging framework
        """
        if len(results) > 0:
            self._log_debug("Validation errors, len = {}".format(len(results)))
            for result in results:
                self.handle_single_validation(result)

    def handle_single_validation(self, result):
        # TODO: If warning, log, if error, log, then throw
        msg_row = "{}".format(result)
        self.step_logger_service.log(msg_row)
        self._log_debug("{}".format(msg_row))

    def _log_debug(self, msg):
        if self.logger is not None:
            self.logger.debug(msg)
