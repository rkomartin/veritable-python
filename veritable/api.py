import os
import simplejson
from .connection import Connection
from .exceptions import *
from .utils import _make_table_id, _make_analysis_id, _format_url

BASE_URL = "https://api.priorknowledge.com/"

def connect(api_key=None, api_base_url=None, ssl_verify=True,
        disable_gzip=False, debug=False):
    if api_key is None:
        api_key = os.getenv("VERITABLE_KEY")
    if api_base_url is None:
        api_base_url = os.getenv("VERITABLE_URL") or BASE_URL
    connection = Connection(api_key, api_base_url, ssl_verify,
            disable_gzip, debug)
    try:
        connection_test = connection.get("/")
    except simplejson.JSONDecodeError:
        raise(APIConnectionException(api_base_url))

    if not connection_test['status'] == "SUCCESS" or not isinstance(connection_test['entropy'], float):
        raise(APIConnectionException(api_base_url))
    return API(connection)

def handle_api_error(err):
    raise Exception(err)

class API:
    def __init__(self, connection):
        self.connection = connection
        self.url = connection.api_base_url

    def __str__(self):
        return "Veritable API at " + self.url

    def table_exists(self, table_id):
        try:
            self.get_table(table_id)
        except:
            return False
        else:
            return True

    def get_tables(self):
        """Return the Veritable tables available to the user."""
        r = self.connection.get("tables")
        return [Table(self.connection, t) for t in r["tables"]]

    def get_table(self, table_id):
        """Get a table from the collection by its id."""
        r = self.connection.get(_format_url("tables", table_id))
        return Table(self.connection, r)
    
    def create_table(self, table_id=None, description="", force=False):
        """Create a table with the given id."""    
        if table_id is None:
            autogen = True
            table_id = _make_table_id()
        else:
            autogen = False
        if self.table_exists(table_id):
            if autogen:
                return self.create_table(table_id=None,
                            description=description, force=False)
            if not force:
                raise DuplicateTableException(table_id)
            else:
                self.delete_table(table_id)
        r = self.connection.post("tables",
                data = {"_id": table_id, "description": description})
        return Table(self.connection, r)
    
    def delete_table(self, table_id):
        """Delete a table from the collection by its id."""
        r = self.connection.get(_format_url("tables", table_id))
        return Table(self.connection, r).delete()

class Table:
    def __init__(self, connection, data):
        self.connection = connection
        self.has_been_deleted = False
        self.id = data["_id"]
        self.links = {"self": data["links"]["self"],
                      "analyses": data["links"]["analyses"],
                      "rows": data["links"]["rows"]}    
                      
    def __str__(self):
        return "Veritable table at " + self.links["self"]

    def _still_alive(self):
        """Check to make sure the table still exists."""
        if self.has_been_deleted:
            raise DeletedTableException()
            
    def _get_state(self):
        """Get the state of the table."""
        self._still_alive()
        return self.connection.get(self.links["self"])
        
    def delete(self):
        """Delete the table."""
        self._still_alive()
        self.has_been_deleted = True
        return self.connection.delete(self.links["self"])
        
    def upload_row(self, row):
        """Add a row to the table."""
        self._still_alive()
        if "_id" not in row:
            raise MissingRowIDException()
        else:
            row_id = row["_id"]
            if not isinstance(row_id, basestring):
                raise TypeError("Row id must be a string")
        return self.connection.put(_format_url(self.links["rows"], row_id),
                row)
        
    def batch_upload_rows(self, rows):
        """Batch add rows to the table."""
        self._still_alive()
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
        data = {'action': 'put', 'rows': rows}
        return self.connection.post(self.links["rows"], data)

    def get_row(self, row_id):
        """Get a row from the table by its id."""
        self._still_alive()
        return self.connection.get(_format_url(self.links["rows"], row_id))

    def get_rows(self):
        """Get the rows of the table."""
        self._still_alive()
        return self.connection.get(self.links["rows"])["rows"]

    def delete_row(self, row_id):
        """Delete a row from the table by its id."""
        self._still_alive()
        return self.connection.delete(_format_url(self.links["rows"], row_id))

    def delete_rows(self, rows):
        """Batch delete rows from the table."""
        self._still_alive()
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
        data = {'action': 'delete', 'rows': rows}
        return self.connection.post(self.links["rows"], data)

    def get_analyses(self):
        """Get the analyses corresponding to the table."""
        self._still_alive()
        r = self.connection.get(self.links["analyses"])
        return [Analysis(self.connection, a) for a in r["data"]]

    def get_analysis(self, analysis_id):
        """Get an analysis corresponding to the table by its id."""
        self._still_alive()
        r = self.connection.get(_format_url(self.links["analyses"],
                analysis_id))
        return Analysis(self.connection, r)

    def delete_analysis(self, analysis_id):
        """Delete an analysis corresponding to the table by its id."""
        self._still_alive()
        r = self.connection.get(_format_url(self.links["analyses"],
                analysis_id))
        return Analysis(self.connection, r).delete()

    def _analysis_exists(self, analysis_id):
        """Test if an analysis with a given id already exists."""
        try:
            self.get_analysis(analysis_id)
        except:
            return False
        else:
            return True

    def create_analysis(self, schema, analysis_id=None, description="",
                        type="veritable", force=False):
        """Create a new analysis for the table."""
        self._still_alive()
        if type != "veritable":
            raise InvalidAnalysisTypeException()
        if analysis_id is None:
            autogen = True
            analysis_id = _make_analysis_id()
        else:
            autogen = False
        if self._analysis_exists(analysis_id):
            if autogen:
                return self.create_analysis(schema=schema,
                        description=description, analysis_id=None,
                        type=type, force=False)
            if not force:
                raise DuplicateAnalysisException(analysis_id)
            else:
                self.delete_analysis(analysis_id)
        r = self.connection.post(self.links["analyses"],
                data = {"_id": analysis_id, "description": description,
                        "type": type, "schema": schema})
        return Analysis(self.connection, r)
                                        
class Analysis:
    def __init__(self, connection, doc):
        self.connection = connection
        self._doc = doc

    def _link(self, name):
        if name not in self._doc['links']:
            raise VeritableError('analysis has no {0} link'.format(name))
        return self._doc['links'][name]

    def state(self):
        return self._doc['state']

    @property
    def error(self):
        if self.state() != 'failed':
            return None
        else:
            return self._doc['error']

    def update(self):
        self._doc = self.connection.get(self._link('self'))

    def delete(self):
        return self.connection.delete(self._link('self'))

    def get_schema(self):
        return self.connection.get(self._link('schema'))

    def predict(self, row, count=10):
        return self.connection.post(
            self._link('predict'),
            data={'data': row, 'count': count})
