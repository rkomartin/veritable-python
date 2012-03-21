class VeritableError(Exception):
    """Base class for exceptions in veritable-python."""
    pass


class APIKeyException(VeritableError):
    """Raised if an API key is not provided when instantiating a Veritable
        connection."""
    def __init__(self):
        self.value = """Must provide an API key to instantiate a Veritable \
        connection"""

    def __str__(self):
        return repr(self.value)


class APIBaseURLException(VeritableError):
    """Raised if a base URL is not provided when instantiating a Veritable
        connection."""
    def __init__(self):
        self.value = """Must provide a base URL to instantiate a Veritable \
        connection"""

    def __str__(self):
        return repr(self.value)


class APIConnectionException(VeritableError):
    """Raised if a Veritable server is not found at the base URL."""
    def __init__(self, key, url, e=None):
        if e:
            self.value = ("""No Veritable server found at {0} using API \
            key {1}""".format(url,key), e)
        else:
            self.value = """No Veritable server found at {0} using API \
            key {1}""".format(url,key)

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


class DuplicateTableException(VeritableError):
    """Raised if an attempt is made to create a table that already exists,
        without overwriting it."""
    def __init__(self, table_id):
        self.value = """Table with id """ + table_id + """ already exists! \
        Set force=True to override."""

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
    def __init__(self, val="unknown"):
        self.value = """Error reported by server: """ + val

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
