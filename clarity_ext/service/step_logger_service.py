import logging
import time
from clarity_ext.service.file_service import SharedFileNotFound


class StepLoggerService:
    """
    Provides support for logging to shared files in a step.
    """

    def __init__(self, step_logger_name, file_service, raise_if_not_found=False, append=True, extension="log"):
        self.core_logger = logging.getLogger(__name__)
        self.step_logger_name = step_logger_name
        self.file_service = file_service

        # Use Windows line endings for now, since most clients are currently Windows.
        # TODO: This should be configurable.
        self.NEW_LINE = "\r\n"
        try:
            mode = "ab" if append else "wb"
            self.step_log = self.file_service.local_shared_file(step_logger_name, extension=extension,
                                                                mode=mode, modify_attached=True)
        except SharedFileNotFound:
            if raise_if_not_found:
                raise
            else:
                self.step_log = None

    def _log(self, level, msg):
        if self.step_log:
            # TODO: Get formatting from the core logging framework
            if level:
                self.step_log.write("{} - {} - {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), logging.getLevelName(level), msg + self.NEW_LINE))
            else:
                self.step_log.write("{}".format(msg + self.NEW_LINE))

        # Forward to the core logger:
        if level:
            self.core_logger.log(level, msg)

    def error(self, msg):
        self._log(logging.ERROR, msg)

    def warning(self, msg):
        self._log(logging.WARNING, msg)

    def info(self, msg):
        self._log(logging.INFO, msg)

    def log(self, msg):
        # Logs without forwarding to the core logger, and without any formatting
        self._log(None, msg)

    def get(self, name):
        # This factory method is added for readability in the extensions.
        return StepLoggerService(name, self.file_service, raise_if_not_found=True, append=False)

