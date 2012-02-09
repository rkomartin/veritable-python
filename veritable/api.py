from .connection import Connection
from .exceptions import *
from .utils import *

BASE_URL = "https://api.priorknowledge.com/"

def veritable_connect(api_key, api_base_url=BASE_URL, ssl_verify=True,
                      disable_gzip=False, debug=False):
    return API(Connection(api_key, api_base_url, ssl_verify,
                          disable_gzip, debug))

def validate_schema(schema):
    for k in schema.keys():
        if not isinstance(k, basestring):
            raise InvalidSchemaException()
    for v in schema.values():
        if not v.keys() == ['type']:
            raise InvalidSchemaException()
        if not len(v.values()) == 1:
            raise InvalidSchemaException()
        if not v.values()[0] in ['boolean', 'categorical', 'real', 'count']:
            raise InvalidSchemaException()

def handle_api_error(err):
    pass

class API:
    def __init__(self, connection):
        self.connection = connection
        self.url = connection.api_base_url
    
    def __str__(self):
        return "Veritable API at " + self.url
        
    def get_tables(self):
        """Return the Veritable tables available to the user."""
        r = self.connection.get("tables")
        return [Table(self.connection, t) for t in r["tables"]]

    def create_table(self, table_id=None, description="", force=False):
        """Create a table with the given id."""    
        if table_id is None:
            autogen = True
            table_id = make_table_id()
        if not force:
            try:
                self.get_table_by_id(table_id)
            except:
                pass
            else:
                if autogen:
                    return self.create_table(table_id=None,
                        description=description, force=False)
                else:
                    raise DuplicateTableException(table_id)
        r = self.connection.put(format_url("tables", table_id),
                                data = {"description": description})
        return Table(self.connection, r)
    
    def get_table_by_id(self, table_id):
        """Get a table from the collection by its id."""
        r = self.connection.get(format_url("tables", table_id))
        return Table(self.connection, r)
    
    def get_table_by_url(self, url):
        """Get a table from the collection by its URL."""
        r = self.connection.get(format_url(url))
        return Table(self.connection, r)

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
            
    def get_state(self):
        """Get the state of the table."""
        self.still_alive()
        return self.connection.get(self.links["self"])
        
    def delete(self):
        """Delete the table."""
        self.still_alive()
        self.has_been_deleted = True
        return self.connection.delete(self.links["self"])
        
    def add_row(self, row, force=False):
        """Add a row to the table."""
        self.still_alive()
        if "_id" not in row:
            raise MissingRowIDException()
        else:
            row_id = row["_id"]
        return self.connection.put(format_url(self.links["rows"], row_id),
                                   row)
        
    def add_rows(self, rows, force=False):
        """Batch add rows to the table."""
        self.still_alive()
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
        data = {'action': 'put', 'rows': rows}
        return self.connection.post(self.links["rows"], data)

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
        return self.connection.get(self.links["rows"])["rows"]

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
        return self.connection.post(self.links["rows"], data)

    def get_analyses(self):
        """Get the analyses corresponding to the table."""
        self.still_alive()
        r = self.connection.get(self.links["analyses"])
        return [Analysis(self.connection, a) for a in r["data"]]

    def get_analysis_by_id(self, analysis_id):
        """Get an analysis corresponding to the table by its id."""
        self.still_alive()
        r = self.connection.get(format_url(self.links["analyses"], analysis_id))
        return Analysis(self.connection, r)

    def get_analysis_by_url(self, url):
        """Get an analysis by its URL."""
        self.still_alive()
        r = self.connection.get(url)
        return Analysis(self.connection, r)

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

    def create_analysis(self, schema, description="",
                        analysis_id=None, type="veritable",
                        force=False):
        """Create a new analysis for the table."""
        self.still_alive()
        if type is not "veritable":
            raise InvalidAnalysisTypeException()
        if analysis_id is None:
            autogen = True
            analysis_id = make_analysis_id()
        if not force:
            try:
                self.get_analysis_by_id(analysis_id)
            except:
                pass
            else:
                if autogen:
                    return self.create_analysis(schema=schema, description=description,
                        analysis_id=None, type=type, force=False)
                else:
                    raise DuplicateAnalysisException(analysis_id)
        r = self.connection.put(format_url(self.links["analyses"], analysis_id),
                                data = {"description": description,
                                        "type": type,
                                        "schema": schema})
        return Analysis(self.connection, r)
                                        
class Analysis:
    def __init__(self, connection, data):
        self.connection = connection
        self.has_been_deleted = False
        self.type = data["type"]
        self.links = {}
        for k in ["self", "schema", "run", "predict"]:
            if k in data["links"]:
                self.links[k] = data["links"][k]
    
    def __str__(self):
        return "Veritable analysis at " + self.links["self"]

    def still_alive(self):
        """Check to make sure the analysis still exists."""
        if self.has_been_deleted:
            raise DeletedAnalysisException()

    def get_state(self):
        """Get the state of the analysis."""
        self.still_alive()
        return self.connection.get(self.links["self"])
    
    def did_not_fail(self):
        data = self.get_state()
        if data["state"] is "failed":
            handle_api_error(data["error"])
    
    def ready_to_predict(self):
        data = self.get_state()
        if "predict" not in self.links:
            raise AnalysisNotReadyException()
    
    def status(self):
        data = self.get_state()
        return data["state"]

    def run(self):
        """Run the analysis."""
        self.still_alive()
        self.did_not_fail()
        return self.connection.post(self.links["run"], {})
    
    def delete(self):
        """"Delete the analysis."""
        self.still_alive()
        return self.connection.delete(self.links["self"])

    def get_schema(self):
        """Get the schema corresponding to the analysis."""
        self.still_alive()
        return self.connection.get(self.links["schema"])

    def predict(self, rows, count=10):
        """Make predictions based on analysis results."""
        self.still_alive()
        self.did_not_fail()
        self.ready_to_predict()
        request = {'rows': rows, 'count': count}
        return self.connection.post(self.links["predict"], data = request)
        