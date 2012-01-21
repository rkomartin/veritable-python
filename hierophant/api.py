from hierophant.connection import Connection
from hierophant.utils import *

BASE_URL = "https://api.priorknowledge.com/"

def veritable_connect(api_key, api_base_url = BASE_URL, ssl_verify = True):
    return API(Connection(api_key, api_base_url, ssl_verify))

class DeletedTableException(Exception):
    def __init__(self):
        self.value = """"Cannot perform operations on a table that has already been deleted"""
    def __str__(self):
        return repr(self.value)

class API:
    def __init__(self, connection):
        self.connection = connection
            
    def tables(self):
        """Return the Veritable tables available to the user."""
        r = self.connection.get("tables")
        return [Table(self.connection, t) for t in r["tables"]]
    
    def create_table(self, table_id = None, description = ""):
        """Create a table with the given id."""    
        if table_id is None:
            table_id = make_table_id()
        r = self.connection.put(format_url("tables", table_id),
                                data = {"description": description})
        return Table(self.connection, r)
    
    def get_table_by_id(self, table_id):
        r = self.connection.get(format_url("tables", table_id))
        return Table(self.connection, r)

class Table:
    def __init__(self, connection, data):
        self.connection = connection
        self.has_been_deleted = False
        if "description" in data:
            self.description = data["description"]
        self.last_updated = data["last_updated"]
        self.links = {"self": data["links"]["self"],
                      "analyses": data["links"]["analyses"],
                      "rows": data["links"]["rows"]}    
                      
    def still_alive(self):
        """Check to make sure the table still exists."""
        if self.has_been_deleted:
            raise DeletedTableException()
            
    def get(self):
        """Get the description of the table."""
        self.still_alive()
        r = self.connection.get(self.links["self"])
        return Table(self.connection, r)
        
    def delete(self):
        """Delete the table."""
        self.still_alive()
        self.has_been_deleted = True
        return self.connection.delete(self.links["self"])
        
    def add_row(self, row):
        """Add a row to the table."""
        self.still_alive()
        if "_id" in row:
            row_id = row["_id"]
        else:
            row_id = make_row_id()
        return self.connection.put(format_url(self.links["rows"], row_id), row)
        
    def add_rows(self, data):
        """Add many rows to the table."""
        self.still_alive()
        return self.connection.post(self.links["rows"], data)

    def get_row(self, row_id):
        """Get a row from the table by its id."""
        self.still_alive()
        return self.connection.get(format_url(self.links["rows"], row_id))

    def get_rows(self):
        """Get the rows of the table."""
        self.still_alive()
        return self.connection.get(self.links["rows"])

    def delete_row(self, row_id):
        """Delete a row from the table by its id."""
        self.still_alive()
        return self.connection.delete(format_url(self.links["rows"], row_id))

    def get_analyses(self):
        """Get the analyses corresponding to the table."""
        self.still_alive()
        r = self.connection.get(self.links["analyses"])
        return [Analysis(self.connection, a) for a in r["data"]]

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
                                        
class Analysis:
    def __init__(self, connection, data):
        self.connection = connection
        self.last_learned = data["last_learned"]
        if "last_updated" in data:
            self.last_updated = data["last_updated"]
        self.links = {"self": data["links"]["self"],
                      "schema": data["links"]["schema"],
                      "learn": data["links"]["learn"],
                      "predict": data["links"]["predict"]}
    def get(self):
        data = self.connection.get(self.links["self"])
        self.last_learned = data["last_learned"]
        if "last_updated" in data:
            self.last_updated = data["last_updated"]
        self.links = {"self": data["links"]["self"],
                      "schema": data["links"]["schema"],
                      "learn": data["links"]["learn"],
                      "predict": data["links"]["predict"]}
        return self
        
    def learn(self):
        return self.connection.post(self.links["learn"])
        
    def delete(self):
        return self.connection.delete(self.links["self"])

    def get_schema(self):
        return self.connection.get(self.links["schema"])
        
    def predict(self, request):
        return self.connection.post(self.links["predict"], data = request)
        