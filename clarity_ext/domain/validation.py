class ValidationType:
    ERROR = 1
    WARNING = 2


class ValidationException(object):
    def __init__(self, msg, validation_type=ValidationType.ERROR):
        self.msg = msg
        self.type = validation_type

    def _repr_type(self):
        if self.type == ValidationType.ERROR:
            return "Error"
        elif self.type == ValidationType.WARNING:
            return "Warning"

    def __repr__(self):
        return "{}: {}".format(self._repr_type(), self.msg)


class ValidationResults(object):
    def __init__(self):
        self.results = list()

    def append(self, validation_exception):
        self.results.append(validation_exception)

    def extend(self, validation_exceptions):
        self.results.extend(validation_exceptions)

    @property
    def warnings(self):
        return filter(lambda result: result.type == ValidationType.WARNING, self.results)

    @property
    def errors(self):
        return filter(lambda result: result.type == ValidationType.ERROR, self.results)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return iter(self.results)

    def __repr__(self):
        return repr(self.results)


class UsageError(Exception):
    """
    Throw a usage error if there is an error with parameters the user can change. Include
    any validation errors if applicable, they will be logged appropriately by the framework.
    """
    def __init__(self, msg, validation_results=None):
        super(UsageError, self).__init__(msg)
        self.validation_results = validation_results
