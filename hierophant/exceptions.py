class APIKeyException(Exception):
    def __init__(self):
        self.value = """Must provide an API key to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)

class APIBaseURLException(Exception):
    def __init__(self):
        self.value = """Must provide an base URL to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)

class DeletedTableException(Exception):
    def __init__(self):
        self.value = """"Cannot perform operations on a table that has already been deleted"""
    def __str__(self):
        return repr(self.value)

class DeletedAnalysisException(Exception):
    def __init__(self):
        self.value = """"Cannot perform operations on an analysis that has already been deleted"""
    def __str__(self):
        return repr(self.value)

class MissingRowIDException(Exception):
    def __init__(self):
        self.value = """Rows for deletion must contain row ids in the _id field"""
    def __str__(self):
        return repr(self.value)

class InvalidAnalysisTypeException(Exception):
    def __init__(self):
        self.value = """Invalid analysis type."""
    def __str__(self):
        return repr(self.value)

class InvalidSchemaException(Exception):
    def __init__(self):
        self.value = """Invalid schema specification."""
    def __str__(self):
        return repr(self.value)

class DuplicateTableException(Exception):
    def __init__(self, table_id):
        self.value = "Table with id" + table_id + "already exists! Set force=True to override."
    def __str__(self):
        return repr(self.value)

class DuplicateAnalysisException(Exception):
    def __init__(self, analysis_id):
        self.value = "Analysis with id" + analysis_id + "already exists! Set force=True to override."
    def __str__(self):
        return repr(self.value)

class DuplicateRowException(Exception):
    def __init__(self, row_id):
        self.value = "Row with id" + row_id + "already exists! Set force=True to override."
    def __str__(self):
        return repr(self.value)