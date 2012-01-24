from hierophant.connection import Connection
from hierophant.utils import *

BASE_URL = "https://api.priorknowledge.com/"

def veritable_connect(api_key, api_base_url = BASE_URL, ssl_verify = True):
    return API(Connection(api_key, api_base_url, ssl_verify))

def validate_schema(schema):
    for k in schema.keys():
        if not isinstance(k, basestring):
            raise Exception()
    for v in schema.values():
        if not v.keys() == ['type'] && (v.values() == 'boolean' ||
          v.values() == 'categorical' || v.values() == 'continuous' ||
          v.values() == 'count')
            raise Exception()

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

class API:
    def __init__(self, connection):
        self.connection = connection
        self.url = connection.api_base_url
    
    def __str__(self):
        return "Veritable API at " + self.url
        
    def get_tables(self):
        """Return the Veritable tables available to the user."""
        r = self.connection.get("tables")
        return [[Table(conn, t), t] for t in r["tables"]]

    def create_table(self, table_id = None, description = ""):
        """Create a table with the given id."""    
        if table_id is None:
            table_id = make_table_id()
        r = self.connection.put(format_url("tables", table_id),
                                data = {"description": description})
        return [Table(self.connection, r), r]
    
    def get_table_by_id(self, table_id):
        """Get a table from the collection by its id."""
        r = self.connection.get(format_url("tables", table_id))
        return [Table(self.connection, r), r]
    
    def get_table_by_url(self, url):
        """Get a table from the collection by its URL."""
        r = self.connection.get(format_urls(url))
        return [Table(self.connection, r), r]

    def delete_table_by_id(self, table_id):
        """Delete a table from the collection by its id."""
        r = self.connection.get(format_url("tables", table_id))
        return Table(self.connection, r).delete()

    def delete_table_by_url(self, url):
        """Delete a table from the collection by its id."""
        r = self.connection.get(url)
        return Table(self.connection, r).delete()

class Table:
    def __init__(self, connection, data):
        self.connection = connection
        self.has_been_deleted = False
        self.links = {"self": data["links"]["self"],
                      "analyses": data["links"]["analyses"],
                      "rows": data["links"]["rows"]}    
                      
    def __str__(self):
        return "Veritable table at " + self.links["self"]

    def still_alive(self):
        """Check to make sure the table still exists."""
        if self.has_been_deleted:
            raise DeletedTableException()
            
    def get(self):
        """Get the description of the table."""
        self.still_alive()
        return self.connection.get(self.links["self"])
        
    def delete(self):
        """Delete the table."""
        self.still_alive()
        self.has_been_deleted = True
        return [self, self.connection.delete(self.links["self"])]
        
    def add_row(self, row):
        """Add a row to the table."""
        self.still_alive()
        if "_id" in row:
            row_id = row["_id"]
        else:
            row_id = make_row_id()
            row["_id"] = row_id
        return [self, self.connection.put(format_url(self.links["rows"], row_id),
                                     row)]
        
    def add_rows(self, rows):
        """Batch add rows to the table."""
        self.still_alive()
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                rows[i]["id"] = make_row_id()
        data = {'action': 'put', 'rows': rows}
        return [self, self.connection.post(self.links["rows"], data)]

    def get_row_by_id(self, row_id):
        """Get a row from the table by its id."""
        self.still_alive()
        return self.connection.get(format_url(self.links["rows"], row_id))

    def get_row_by_url(self, url):
        """Get a row from the table by its url."""
        self.still_alive()
        return self.connection.get(format_url(url))

    def get_rows(self):
        """Get the rows of the table."""
        self.still_alive()
        return self.connection.get(self.links["rows"])

    def delete_row_by_id(self, row_id):
        """Delete a row from the table by its id."""
        self.still_alive()
        return self.connection.delete(format_url(self.links["rows"], row_id))

    def delete_row_by_url(self, url):
        """Delete a row from the table by its url."""
        self.still_alive()
        return self.connection.delete(url)

    def delete_rows(self, rows):
        """Batch delete rows from the table."""
        self.still_alive()
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
        data = {'action': 'delete', 'rows': rows}
        return [self, self.connection.post(self.links["rows"], data)]

    def get_analyses(self):
        """Get the analyses corresponding to the table."""
        self.still_alive()
        r = self.connection.get(self.links["analyses"])
        return [[Analysis(self.connection, a), a] for a in r["data"]]

    def get_analysis_by_id(self, analysis_id):
        """Get an analysis corresponding to the table by its id."""
        self.still_alive()
        r = self.connection.get(format_url(self.links["analyses"], analysis_id))
        return [Analysis(self.connection, r), r]

    def get_analysis_by_url(self, url):
        """Get an analysis by its URL."""
        self.still_alive()
        r = self.connection.get(url)
        return [Analysis(self.connection, r), r]

    def delete_analysis_by_id(self, analysis_id):
        """Delete an analysis corresponding to the table by its id."""
        self.still_alive()
        r = self.connection.get(format_url(self.links["analyses"], analysis_id))
        return Analysis(self.connection, r).delete()

    def delete_analysis_by_url(self, url):
        """Delete an analysis corresponding to the table by its URL."""
        self.still_alive()
        r = self.connection.get(url)
        return Analysis(self.connection, r).delete()

    def create_analysis(self, schema, description = "",
                        analysis_id = None, type = "veritable"):
        """Create a new analysis for the table."""
        self.still_alive()
        if analysis_id is None:
            analysis_id = make_analysis_id()
        r = self.connection.put(format_url(self.links["analyses"], analysis_id),
                                data = {"description": description,
                                        "type": type,
                                        "schema": schema})
        return [Analysis(self.connection, r["data"]), r["data"]]
                                        
class Analysis:
    def __init__(self, connection, data):
        self.connection = connection
        self.has_been_deleted = False
        self.links = {"self": data["links"]["self"],
                      "schema": data["links"]["schema"],
                      "learn": data["links"]["learn"],
                      "predict": data["links"]["predict"]}
    
    def __str__(self):
        return "Veritable analysis at " + self.links["self"]

    def still_alive(self):
        """Check to make sure the analysis still exists."""
        if self.has_been_deleted:
            raise DeletedAnalysisException()

    def get(self):
        """Get the description of the analysis."""
        self.still_alive()
        return self.connection.get(self.links["self"])
        
    def learn(self):
        """Learn the analysis."""
        self.still_alive()
        return [self, self.connection.post(self.links["learn"])]
        
    def delete(self):
        """"Delete the analysis."""
        self.still_alive()
        return [self, self.connection.delete(self.links["self"])]

    def get_schema(self):
        """Get the schema corresponding to the analysis."""
        self.still_alive()
        return self.connection.get(self.links["schema"])
        
    def predict(self, request):
        """Make predictions based on analysis results."""
        self.still_alive()
        return [self, self.connection.post(self.links["predict"], data = request)]
        