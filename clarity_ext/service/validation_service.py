from clarity_ext.service.step_logger_service import StepLoggerService
from clarity_ext.domain.validation import ValidationType

# A step using the validation service must be configured with the
# shared file entry 'Errors and warnings'
ERRORS_AND_WARNING_ENTRY_NAME = "Errors_and_warnings"


class ValidationService:

    def __init__(self, context=None, logger=None, step_logger_name=ERRORS_AND_WARNING_ENTRY_NAME):
        self.logger = logger
        if context:
            self.step_logger_service = StepLoggerService(step_logger_name=step_logger_name,
                                                         file_service=context.file_service,
                                                         raise_if_not_found=False, append=False,
                                                         extension="txt")

    def handle_validation(self, validation_results):
        results = list(validation_results)
        print "S", results

        has_errors = any(r.type == ValidationType.ERROR for r in results)
        has_warnings = any(
            r.type == ValidationType.WARNING for r in results)
        results = sorted(results, key=lambda r: r.type)
        if len(results) > 0:
            self._log_debug(
                "Validation errors, len = {}".format(len(results)))
            for r in results:
                msg_row = "{}".format(r)
                self.step_logger_service.log(msg_row)
                self._log_debug("{}".format(msg_row))
        return has_errors, has_warnings

    def _log_debug(self, msg):
        if self.logger is not None:
            self.logger.debug(msg)
