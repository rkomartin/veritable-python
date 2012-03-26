class VeritableError(Exception):
    """Base class for exceptions in veritable-python."""
    def __init__(self, value, **kwargs):
        self.value = value
        for k, v in kwargs.items():
            self.__dict__[k] = v

    def __str__(self):
        return repr(self.value)


class MissingRowIDException(VeritableError):
    """Raised when a row ID is not provided."""
    def __init__(self):
        self.value = """Rows must contain row ids in the _id field"""

    def __str__(self):
        return repr(self.value)


class InvalidIDException(VeritableError):
    """Raised if an invalid ID is provided."""
    def __init__(self, s=None):
        if s:
            self.value = """Specified id """ + s + """ is invalid: \
            alphanumeric, underscore, and hyphen only!"""
        else:
            self.value = """Specified id is invalid: alphanumeric, \
            underscore, and hyphen only!"""

    def __str__(self):
        return repr(self.value)


class InvalidAnalysisTypeException(VeritableError):
    """Raised if an invalid analysis type is specified."""
    def __init__(self):
        self.value = """Invalid analysis type."""

    def __str__(self):
        return repr(self.value)


class InvalidSchemaException(Exception):
    """Raised if an invalid schema is provided."""
    def __init__(self, msg="""Invalid schema specification.""", col=None):
        self.value = msg
        self.col = col

    def __str__(self):
        return repr(self.value)


class DuplicateAnalysisException(VeritableError):
    """Raised if an attempt is made to create an analysis that already exists,
        without overwriting it."""
    def __init__(self, analysis_id):
        self.value = """Analysis with id """ + analysis_id + """ already \
        exists! Set force=True to override."""

    def __str__(self):
        return repr(self.value)


class ServerException(VeritableError):
    """Raised when an error is returned by the Veritable server."""
    def __init__(self, val="unknown", status=None, code=None,
        message=None):
        self.value = """Error reported by server: """ + val
        self.status = status
        self.code = code
        self.message = message

    def __str__(self):
        return repr(self.value)


class DataValidationException(VeritableError):
    """Raised when invalid data is passed to the validation utility
        functions."""
    def __init__(self, msg, row=None, col=None):
        self.value = msg
        self.row = row
        self.col = col

    def __str__(self):
        return repr(self.value)


class MissingLinkException(VeritableError):
    """Raised when a link is missing from the JSON doc of a resource."""
    def __init__(self, resource, name):
        self.value = """{0} instance is missing link to {1}""".format(resource,
            name)

    def __str__(self):
        return repr(self.value)


class AnalysisNotReadyException(VeritableError):
    """Raised when predictions are made from an analysis that is
    still running."""
    def __init__(self, id):
        self.value = """Analysis with id {0} is still running and \
        not yet ready to predict""".format(id)

    def __str__(self):
        return repr(self.value)


class AnalysisFailedException(VeritableError):
    """Raised when predictions are made from an analysis that has failed."""
    def __init__(self, id, e):
        self.value = """Analysis with id {0} has failed and cannot \
        predict: {1}""".format(id, e)

    def __str__(self):
        return repr(self.value)
