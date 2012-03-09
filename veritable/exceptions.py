class VeritableError(Exception):
    pass

class APIKeyException(VeritableError):
    def __init__(self):
        self.value = """Must provide an API key to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)

class APIBaseURLException(VeritableError):
    def __init__(self):
        self.value = """Must provide an base URL to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)

class APIConnectionException(VeritableError):
    def __init__(self, url):
        self.value = """No Veritable server at """ + url
    def __str__(self):
        return repr(self.value)

class DeletedTableException(VeritableError):
    def __init__(self):
        self.value = """"Cannot perform operations on a table that has already been deleted"""
    def __str__(self):
        return repr(self.value)

class DeletedAnalysisException(VeritableError):
    def __init__(self):
        self.value = """"Cannot perform operations on an analysis that has already been deleted"""
    def __str__(self):
        return repr(self.value)

class MissingRowIDException(VeritableError):
    def __init__(self):
        self.value = """Rows must contain row ids in the _id field"""
    def __str__(self):
        return repr(self.value)

class InvalidIDException(VeritableError):
    def __init__(self, s=None):
        if s:
            self.value = """Specified id """ + s + """ is invalid: alphanumeric and underscore only!"""
        else:
            self.value = """Specified id is invalid: alphanumeric and underscore only!"""
    def __str__(self):
        return repr(self.value)

class InvalidAnalysisTypeException(VeritableError):
    def __init__(self):
        self.value = """Invalid analysis type."""
    def __str__(self):
        return repr(self.value)

class InvalidSchemaException(Exception):
    def __init__(self, msg="""Invalid schema specification.""", col=None):
        self.value = msg
        self.col = col
    def __str__(self):
        return repr(self.value)

class DuplicateTableException(VeritableError):
    def __init__(self, table_id):
        self.value = "Table with id " + table_id + " already exists! Set force=True to override."
    def __str__(self):
        return repr(self.value)

class DuplicateAnalysisException(VeritableError):
    def __init__(self, analysis_id):
        self.value = "Analysis with id " + analysis_id + " already exists! Set force=True to override."
    def __str__(self):
        return repr(self.value)

class AnalysisNotReadyException(VeritableError):
    def __init__(self):
        self.value = "Analysis is not ready for predictions."
    def __str__(self):
        return repr(self.value)

class ServerException(VeritableError):
    def __init__(self, val="unknown"):
        self.value = "Error reported by server: " + val
    def __str__(self):
        return repr(self.value)

class InvalidPredictionRequest(VeritableError):
    def __init__(self, msg):
        self.value = msg
    def __str__(self):
        return repr(self.value)

class DataValidationException(VeritableError):
    def __init__(self, msg, row=None, col=None):
        self.value = msg
        self.row = row
        self.col = col
    def __str__(self):
        return repr(self.value)

