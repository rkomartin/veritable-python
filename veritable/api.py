import os
import simplejson
from .connection import Connection
from .exceptions import (APIConnectionException, DuplicateTableException,
    MissingRowIDException, InvalidAnalysisTypeException,
    DuplicateAnalysisException)
from urllib import quote_plus
from .utils import _make_table_id, _make_analysis_id, _check_id

BASE_URL = "https://api.priorknowledge.com/"


def connect(api_key=None, api_base_url=None, ssl_verify=True,
        enable_gzip=True, debug=False):
    """Returns an instance of the Veritable API."""
    if api_key is None:
        api_key = os.getenv("VERITABLE_KEY")
    if api_base_url is None:
        api_base_url = os.getenv("VERITABLE_URL") or BASE_URL
    connection = Connection(api_key=api_key, api_base_url=api_base_url,
            ssl_verify=ssl_verify, enable_gzip=enable_gzip, debug=debug)
    try:
        connection_test = connection.get("/")
    except simplejson.JSONDecodeError:
        raise(APIConnectionException(api_base_url))

    status = connection_test['status']
    entropy = connection_test['entropy']
    if status =! "SUCCESS" or not isinstance(entropy, float):
        raise(APIConnectionException(api_base_url))
    return API(connection)


class API:
    """Gives access to the collection of tables availabe to the user."""
    def __init__(self, connection):
        self._conn = connection
        self._url = connection.api_base_url

    def __str__(self):
        return "<veritable.API url='" + self._url + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        # Retrieves a subresource by name
        if name not in self._doc['links']:
            raise VeritableError('api has no {0} link'.format(name))
        return self._doc['links'][name]

    def table_exists(self, table_id):
        """Checks if a table with the specified id is available to the user."""
        try:
            self.get_table(table_id)
        except:
            return False
        else:
            return True

    def get_tables(self):
        """Returns a list of the tables available to the user."""
        r = self._conn.get("tables")
        return [Table(self._conn, t) for t in r["tables"]]

    def get_table(self, table_id):
        """Gets a table from the collection by its id."""
        r = self._conn.get("/tables/{0}".format(quote_plus(table_id)))
        return Table(self._conn, r)

    def create_table(self, table_id=None, description="", force=False):
        """Creates a new table with the given id."""
        if table_id is None:
            autogen = True
            table_id = _make_table_id()
        else:
            _check_id(table_id)
            autogen = False
        if self.table_exists(table_id):
            if autogen:
                return self.create_table(table_id=None,
                            description=description, force=False)
            if not force:
                raise DuplicateTableException(table_id)
            else:
                self.delete_table(table_id)
        r = self._conn.post("/tables",
                data={"_id": table_id, "description": description})
        return Table(self._conn, r)

    def delete_table(self, table_id):
        """Deletes a table from the collection by its id."""
        return self._conn.delete("/tables/{0}".format(quote_plus(table_id)))


class Table:
    """Gives access to the rows and analyses associated with a given table."""
    def __init__(self, connection, doc):
        self._conn = connection
        self._doc = doc

    def __str__(self):
        return "<veritable.Table id='" + self.id + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        # Retrieves a subresource by name
        if name not in self._doc['links']:
            raise VeritableError('table has no {0} link'.format(name))
        return self._doc['links'][name]

    def _analysis_exists(self, analysis_id):
        # Checks if an analysis with a given id already exists.
        try:
            self.get_analysis(analysis_id)
        except:
            return False
        else:
            return True

    @property
    def id(self):
        """The id of the table."""
        return self._doc['_id']

    def delete(self):
        """Deletes the table."""
        return self._conn.delete(self._link("self"))

    def get_row(self, row_id):
        """Gets a row from the table by its id."""
        return self._conn.get("{0}/{1}".format(self._link("rows").rstrip("/"),
            quote_plus(row_id)))

    def get_rows(self):
        """Gets all the rows of the table."""
        return self._conn.get(self._link("rows"))["rows"]

    def upload_row(self, row):
        """Adds a row to the table or updates an existing row."""
        if "_id" not in row:
            raise MissingRowIDException()
        else:
            row_id = row["_id"]
            _check_id(row_id)
            if not isinstance(row_id, basestring):
                raise TypeError("Row id must be a string")
        return self._conn.put("{0}/{1}".format(self._link("rows").rstrip("/"),
            quote_plus(row_id)), row)

    def batch_upload_rows(self, rows):
        """Batch adds rows to the table or updates existing rows."""
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
            _check_id(rows[i]["_id"])
        data = {'action': 'put', 'rows': rows}
        return self._conn.post(self._link("rows"), data)

    def delete_row(self, row_id):
        """Deletes a row from the table by its id."""
        return self._conn.delete("{0}/{1}".format(
            self._link("rows").rstrip("/"), quote_plus(row_id)))

    def batch_delete_rows(self, rows):
        """Batch deletes rows from the table."""
        for i in range(len(rows)):
            if not "_id" in rows[i]:
                raise MissingRowIDException()
        data = {'action': 'delete', 'rows': rows}
        return self._conn.post(self._link("rows"), data)

    def get_analyses(self):
        """Gets all the analyses corresponding to the table."""
        r = self._conn.get(self._link("analyses"))
        return [Analysis(self._conn, a) for a in r["analyses"]]

    def get_analysis(self, analysis_id):
        """Gets an analysis corresponding to the table by its id."""
        r = self._conn.get("{0}/{1}".format(self._link("analyses").rstrip("/"),
            quote_plus(analysis_id)))
        return Analysis(self._conn, r)

    def delete_analysis(self, analysis_id):
        """Deletes an analysis corresponding to the table by its id."""
        self._conn.delete("{0}/{1}".format(self._link("analyses").rstrip("/"),
            quote_plus(analysis_id)))

    def create_analysis(self, schema, analysis_id=None, description="",
                        type="veritable", force=False):
        """Creates a new analysis of the table."""
        if type != "veritable":
            raise InvalidAnalysisTypeException()
        if analysis_id is None:
            autogen = True
            analysis_id = _make_analysis_id()
        else:
            _check_id(analysis_id)
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
        r = self._conn.post(self._link("analyses"),
                data={"_id": analysis_id, "description": description,
                      "type": type, "schema": schema})
        return Analysis(self._conn, r)


class Analysis:
    """Gives access to the schema associated with an analysis
        and makes predictions."""
    def __init__(self, connection, doc):
        self._conn = connection
        self._doc = doc

    def __str__(self):
        return "<veritable.Analysis id='" + self.id + "'>"

    def __repr__(self):
        return self.__str__()

    def _link(self, name):
        if name not in self._doc['links']:
            raise VeritableError('analysis has no {0} link'.format(name))
        return self._doc['links'][name]

    @property
    def id(self):
        """The id of the analysis."""
        return self._doc['_id']

    @property
    def state(self):
        """The state of the analysis: one of 'succeeded', 'failed',
            or 'running'."""
        return self._doc['state']

    @property
    def error(self):
        """The error, if any, encountered by the analysis."""
        if self.state != 'failed':
            return None
        else:
            return self._doc['error']

    def update(self):
        """Manually updates the analysis, checking whether it has succeeded
            or failed."""
        self._doc = self._conn.get(self._link('self'))

    def delete(self):
        """Deletes the analysis."""
        return self._conn.delete(self._link('self'))

    def get_schema(self):
        """Gets the schema of the analysis."""
        return self._conn.get(self._link('schema'))

    def predict(self, row, count=10):
        """Makes predictions from the analysis."""
        if self.state == 'running':
            self.update()
        if self.state == 'succeeded':
            return self._conn.post(
            self._link('predict'),
            data={'data': row, 'count': count})
        elif self.state == 'running':
            raise VeritableError('Analysis is not yet complete!')
        elif self.state == 'failed':
            raise VeritableError(self.error)
