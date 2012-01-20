from hierophant.connection import Connection
from hierophant.utils import *

def veritable_connect(api_key):
    return API(Connection(api_key))

class DeletedTableException(Exception):
    def __init__(self):
        self.value = """"Cannot perform operations on a table that has already
                         been deleted"""
    def __str__(self):
        return repr(self.value)

class API:
    def __init__(self, connection):
        self.connection = connection
            
    def tables(self):
        """Return the Veritable tables available to the user."""
        r = self.connection.get()
        return [Table(self.connection, t) for t in r["data"]]
    
    def create_table(self, table_id = None, description = ""):
        """Create a table with the given id."""    
        if table_id is None:
            table_id = make_table_id()
        r = self.connection.put(join_url(self.connection.BASE_URL, table_id),
                                data = {"description": description})
        return Table(self.connection, r)

class Table:
    def __init__(self, connection, data):
        self.connection = connection
        if "description" in data:
            self.description = data["description"]
        self.id = data["_id"]
        self.last_updated = data["last_updated"]
        self.links = {"self": data["links"]["self"],
                      "analyses": data["links"]["analyses"],
                      "rows": data["links"]["rows"]}    
                      
    def still_alive(self):
        """Check to make sure the table still exists."""
        if self.has_been_deleted:
            raise DeletedTableException()
            
    def get(self):
        self.still_alive()
        data = self.connection.get(self.links["self"])
        if "description" in data:
            self.description = data["description"]
        self.id = data["_id"]
        self.last_updated = data["last_updated"]
        self.links = {"self": data["links"]["self"],
                      "analyses": data["links"]["analyses"],
                      "rows": data["links"]["rows"]}    
        return self
        
    def delete(self):
        self.still_alive()
        self.has_been_deleted = True
        return self.connection.delete(self.links["self"])
        
    def add_row(self, row):
        self.still_alive()
        if "_id" in row:
            row_id = row["_id"]
        else:
            row_id = make_row_id()
        return self.connection.put(join_url(self.links["rows"], row_id), row)
        
    def add_rows(self, data):
        self.still_alive()
        return self.connection.post(self.links["rows"], data)
        
    def get_row(self, row_id):
        self.still_alive()
        return self.connection.get(join_url(self.links["rows"], row_id))

    def get_rows(self):
        self.still_alive()
        return self.connection.get(self.links["rows"])
        
    def delete_row(self, row_id):
        self.still_alive()
        return self.connection.delete(join_url(self.links["rows"], row_id))
        
    def get_analyses(self):
        self.still_alive()
        r = self.connection.get(self.links["analyses"])
        return [Analysis(self.connection, a) for a in r["data"]]
    
    def create_analysis(self, schema, description = "",
                        analysis_id = None, type = "veritable"):
        self.still_alive()
        if analysis_id is None:
            analysis_id = make_analysis_id()
        r = self.connection.put(join_url(self.links["analyses"], analysis_id),
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
        