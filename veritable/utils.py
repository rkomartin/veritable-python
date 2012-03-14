"""Utility functions for working with veritable-python.

See also: https://dev.priorknowledge.com/docs/client/python

"""

import time
import uuid
from math import floor
from random import shuffle
from urlparse import urlparse
import csv
import re
import string
from .exceptions import *


_alphanumeric = re.compile("^[-_a-zA-Z0-9]+$")

def _check_id(id):
    # Note that this will choke on unicode ids
    if _alphanumeric.match(id) is None or id[-1] == "\n":
        raise InvalidIDException(id)


def _make_table_id():
    # Autogenerate id
    return uuid.uuid4().hex


def _make_analysis_id():
    # Autogenerate id
    return uuid.uuid4().hex


def _url_has_scheme(url):
    # Check if a URL includes a scheme
    return urlparse(url)[0] is not ""


def wait_for_analysis(a, poll=2):
    """Waits for a running analysis to succeed or fail.

    Arguments:
    a -- the Analysis object for which to wait
    poll -- the number of seconds to wait between updates (default 2)

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    while a.state == 'running':
        time.sleep(poll)
        a.update()


def split_rows(rows, frac=0.5):
    """Splits a list of dicts representing a dataset into two sets.

    Returns a tuple of lists of dicts, containing (floor(len(rows) * frac),
    1 - floor(len(rows) * frac)) row dicts respectively, sampled at
    random.

    Arguments:
    rows -- the list of dicts representing the dataset to split
    frac -- the fraction of rows to split by (default 0.5)

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    N = len(rows)
    inds = range(N)
    shuffle(inds)
    border_ind = int(floor(N * frac))
    train_dataset = [rows[i] for i in inds[0:border_ind]]
    test_dataset = [rows[i] for i in inds[border_ind:]]
    return train_dataset, test_dataset


def _validate_schema(schema):
    # Checks whether a schema is well formed and raises an
    # InvalidSchemaException if not.
    valid_types = ['boolean', 'categorical', 'real', 'count']
    for k in schema.keys():
        if not isinstance(k, basestring):
            raise InvalidSchemaException()
        v = schema[k]
        if not ('type' in v.keys()):
            raise InvalidSchemaException("""Column '""" + k +
                """' does not have a 'type' specified. Please specify
                'type' as one of ['""" + string.join(valid_types, "', '") +
                """']""", col=k)
        if not v['type'] in valid_types:
            raise InvalidSchemaException("""Column '""" + k + """' type '""" +
                v['type'] + """' is not valid. Please specify 'type' as
                one of ['""" + string.join(valid_types,"', '") + """']""",
                col=k)


def validate_schema(schema):
    """Checks if an analysis schema is well-formed.

    Returns True if the schema is well-formed, False otherwise.

    Note that this function does not check the schema against the dataset. To
    validate against a dataset, use validate_data.

    Arguments:
    schema -- the schema to validate as a Python dict

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    try:
        _validate_schema(schema)
    except:
        return False
    else:
        return True


def make_schema(schema_rule, headers=None, rows=None):
    """Constructs an analysis schema from a schema rule.

    Returns an analysis schema as a Python dict.

    Arguments:
    schema_rule -- a list of lists in the form:
        [['a_regex_to_match', 'datatype'], ['another_regex', 'datatype'], ...] 
        Earlier rules will match before later rules.
    headers -- a list of column names against which to match. (default: None)
        If headers is not provided, column names will be read from the rows
        argument. Either headers or rows must be provided, or an Exception
        will be raised.
    rows -- a list of row dicts from which column names will be extracted if
        headers are not specified. (default: None) 

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    if headers is None and rows is None:
        raise Exception("Either headers or rows must be provided")
    if headers is None:
        headers = set()
        for r in rows:
            headers = headers.union(r.keys())
    schema = {}
    for i in range(len(schema_rule)):
        schema_rule[i][0] = re.compile(schema_rule[i][0])
    for c in headers:
        for (r, t) in schema_rule:
            if r.match(c):
                schema[c] = t
                break
    return schema


# Dialects: csv.excel_tab, csv.excel
def write_csv(rows, filename, dialect=csv.excel, na_val=''):
    """Writes a list of row dicts to disk as .csv

    Does not support Unicode values in row dicts.

    Arguments:
    rows -- the list of row dicts to write
    filename -- the filename to which to write
    dialect -- a subclass of csv.Dialect (default: csv.excel)
    na_val -- columns that are missing in a row or that are set to None will
        be written out as this value (default: '')

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    headers = set()
    for r in rows:
        headers = headers.union(r.keys())
    headers = list(headers)
    headers.sort()
    with open(filename,'wb') as outFile:
        writer = csv.writer(outFile, dialect=dialect)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([(na_val if not(r.has_key(c))
                              else na_val if r[c] is None
                              else str(r[c])) for c in headers])

# Dialects: csv.excel_tab, csv.excel
def read_csv(filename, id_col=None, dialect=None, na_vals=['']):
    """Reads a .csv from disk into a list of row dicts.

    Returns a list of dicts representing the rows in the .csv file.

    Does not support .csvs that contain Unicode values.

    Arguments:
    filename -- the .csv file to read from
    id_col -- the column, if any, containing unique row ids (default: None)
        If None, the rows will be numbered sequentially; otherwise, this
        column will be renamed to '_id' (as required by the row upload
        functions). If id_col is specified, but ids are missing for some rows,
        then a VeritableException will be raised.
    dialect -- a subclass of csv.Dialect to use in reading the .csv file
        (default: None) If None, read_csv will try to sniff the dialect using
        csv.Sniffer.
    na_vals -- a list of values to treat as NA (default: ['']) Each row dict
        will contain only those columns in which these values do not occur.

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    table = []
    with open(filename) as f:
        if dialect == None:
            dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.reader(f, dialect)
        header = [h.strip() for h in reader.next()]
        if '_id' in header:
            id_col = '_id'
        if id_col is None:
            rid = 0
        for row in reader:
            r = {}
            for i in range(min(len(header), len(row))):
                val = row[i].strip()
                if not(val in na_vals):
                    if(header[i] == id_col):
                        r['_id'] = val
                    else:
                        r[header[i]] = val
                else:
                    if header[i] == id_col:
                        raise VeritableException("Missing id for row" + str(i))
            if id_col == None:
                rid = rid + 1
                r['_id'] = str(rid)
            table.append(r)
    return table;

def validate_data(rows, schema, convert_types=False, remove_nones=False,
    remove_invalids=False, map_categories=False, assign_ids=False,
    remove_extra_fields=False):
    """Validates a list of row dicts against an analysis schema.

    Raises a DataValidationException containing further details if the data
    does not validate against the schema.

    WARNING: Setting the optional arguments convert_types, remove_nones,
    remove_invalids, reduce_categories, assign_ids, or remove_extra_fields to
    True will cause destructive updates to be performed on the rows argument.
    If validate_data raises an exception, values in some rows may be converted
    while others are left in their original state.

    Arguments:
    rows -- the list of row dicts to validate
    schema -- an analysis schema specifying the types of the columns appearing
        in the rows being validated
    convert_types -- controls whether validate_data will attempt to convert
        cells in a column to be of the correct type (default: False)
    remove_nones -- controls whether validate_data will automatically remove
        cells containing the value None (default: False)
    remove_invalids -- controls whether validate_data will automatically
        remove cells that are invalid for a given column (default: False)
    reduce_categories -- controls whether validate_data will automatically
        reduce the number of categories in categorical columns with too many
        categories (default: False) If True, the largest categories in a
        column will be preserved, up to the allowable limit, and the other
        categories will be binned as "Other".
    assign_ids -- controls whether validate_data will automatically assign new
        ids to the rows (default: False) If True, rows will be numbered
        sequentially. If the rows have an existing '_id' column,
        remove_extra_fields must also be set to True to avoid raising a
        DataValidationException.
    remove_extra_fields -- controls whether validate_data will automatically
        remove columns that are not contained in the schema (default: False)
        If assign_ids is True, will also remove the '_id' column. 

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(rows, schema, convert_types=convert_types,
        allow_nones=False, remove_nones=remove_nones,
        remove_invalids=remove_invalids, map_categories=map_categories,
        has_ids=True, assign_ids=assign_ids, allow_extra_fields=True,
        remove_extra_fields=remove_extra_fields)

def validate_predictions(predictions, schema, convert_types=False,
    remove_invalids=False, remove_extra_fields=False):
    """Validates a predictions request against an analysis schema.

    Raises a DataValidationException containing further details if the request
    does not validate against the schema.

    WARNING: Setting the optional arguments convert_types, remove_invalids, 
    or remove_extra_fields to True will cause destructive updates to be
    performed on the predictions argument. If validate_predictions raises an
    exception, some values may be converted while others are left in their
    original state.

    Arguments:
    predictions -- the predictions request to validate
    schema -- an analysis schema specifying the types of the columns appearing
        in the predictions request being validated
    convert_types -- controls whether validate_data will attempt to convert
        cells in a column to be of the correct type (default: False)
    remove_invalids -- controls whether validate_data will automatically
        remove cells that are invalid for a given column (default: False)
    remove_extra_fields -- controls whether validate_data will automatically
        remove columns that are not contained in the schema (default: False)

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(predictions, schema, convert_types=convert_types,
        allow_nones=True, remove_nones=False, remove_invalids=remove_invalids,
        map_categories=False, has_ids=False, assign_ids=False,
        allow_extra_fields=False, remove_extra_fields=remove_extra_fields)

def _validate(rows, schema, convert_types, allow_nones, remove_nones,
    remove_invalids, map_categories, has_ids, assign_ids, allow_extra_fields,
    remove_extra_fields):
    # Validates data against an analysis schema
    _validate_schema(schema)
    unique_ids = {}
    category_counts = {}
    TRUE_STRINGS = ['true', 't', 'yes', 'y']
    FALSE_STRINGS = ['false', 'f', 'no', 'n']
    for i in range(len(rows)):
        r = rows[i]
        if assign_ids:
            r['_id'] = str(i)
        elif has_ids:
            if not(r.has_key('_id')):
                raise DataValidationException("""Row:'""" + str(i) +
                    """' is missing Key:'_id'""", row=i, col='_id')
            if convert_types:
                try:
                    r['_id'] = str(r['_id'])
                except UnicodeDecodeError:
                    raise DataValidationException("""Row:'""" + str(i) +
                        """' Key:'_id' Value:'""" + r['_id'].encode('utf-8') +
                        """' is """ + str(type(r['_id'])) + """, not a str""",
                        row=i, col='_id')
            if not isinstance(r['_id'], str):
                if isinstance(r['_id'], unicode):
                    raise DataValidationException("""Row:'""" + str(i) +
                        """' Key:'_id' Value:'""" + r['_id'].encode('utf-8') +
                        """' is """ + str(type(r['_id'])) + """, not a str""",
                        row=i, col='_id')
                else:
                    raise DataValidationException("""Row:'""" + str(i) +
                        """' Key:'_id' Value:'""" + str(r['_id']) +
                        """' is """ + str(type(r['_id'])) + """, not a str""",
                        row=i, col='_id')
            _check_id(r['_id'])
            if r['_id'] in unique_ids:
                raise DataValidationException("""Row:'""" + str(i) +
                    """' Key:'_id' Value:'""" + str(r['_id']) +
                    """' is not unique, conflicts with Row:'""" +
                    str(unique_ids[r['_id']]) + """'""", row=i, col='_id')
            unique_ids[r['_id']] = i
        elif r.has_key('_id'):
            if remove_extra_fields:
                r.pop('_id')
            else:
                raise DataValidationException("""Row:'""" + str(i) +
                    """' Key:'_id' should not be included""", row=i, col='_id')
        for c in r.keys():
            if not(c == '_id'):
                if not(schema.has_key(c)):
                    if remove_extra_fields:
                        r.pop(c)
                    else:
                        if not(allow_extra_fields):
                            raise DataValidationException("Row:'" + str(i) + 
                                """' Key:'""" + c + """' is not defined in schema""",
                                row=i, col=c)                    
                elif r[c] == None:
                    if remove_nones:
                        r.pop(c)
                    else:
                        if not(allow_nones):
                            raise DataValidationException("""Row:'""" +
                                str(i) + """' Key:'""" + c + """' should be
                                removed because it has value None""", row=i,
                                col=c)                    
                else:
                    coltype = schema[c]['type']
                    if coltype == 'count':
                        if convert_types:                            
                            try:
                                r[c] = int(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not isinstance(r[c], int):
                                raise DataValidationException("""Row:'""" +
                                    str(i) + """' Key:'""" + c + """' Value:'""" +
                                    str(r[c]) + """' is """ + str(type(r[c])) + 
                                    """, not an int""", row=i, col=c)                            
                    elif coltype == 'real':
                        if convert_types:                            
                            try:
                                r[c] = float(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not isinstance(r[c], float):
                                raise DataValidationException("""Row:'""" +
                                    str(i) + """' Key:'""" + c +
                                    """' Value:'""" + str(r[c]) + """' is """ + 
                                    str(type(r[c])) + """, not a float""", row=i,
                                    col=c)
                    elif coltype == 'boolean':
                        if convert_types:                            
                            try:
                                if str(r[c]).strip().lower() in TRUE_STRINGS:
                                    r[c] = True
                                elif str(r[c]).strip().lower() in FALSE_STRINGS:
                                    r[c] = False
                                else:
                                    r[c] = bool(int(r[c]))
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not isinstance(r[c], bool):
                                raise DataValidationException("""Row:'""" + str(i) + 
                                    """' Key:'""" + c + """' Value:'""" + str(r[c]) +
                                    """' is """ + str(type(r[c])) + """, not a bool""",
                                    row=i, col=c)
                    elif coltype == 'categorical':
                        if convert_types:                            
                            try:
                                r[c] = str(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not isinstance(r[c], str):
                                raise DataValidationException("""Row:'""" + str(i) +
                                    """' Key:'""" + c + """' Value:'""" + str(r[c]) +
                                    """' is """ + str(type(r[c])) + """, not a str""",
                                    row=i, col=c)
                            if not(category_counts.has_key(c)):
                                category_counts[c] = {}
                            if not(category_counts[c].has_key(r[c])):
                                category_counts[c][r[c]] = 0
                            category_counts[c][r[c]] = category_counts[c][r[c]] + 1
    MAX_CATS = 256
    for c in category_counts.keys():
        cats = category_counts[c].keys()
        if (len(cats) > MAX_CATS):
            if map_categories:
                cats.sort(key=lambda cat: category_counts[c][cat])
                cats.reverse()
                category_map = {}
                for j in range(len(cats)):
                    if j < (MAX_CATS - 1):
                        category_map[cats[j]] = cats[j]
                    else:
                        category_map[cats[j]] = 'Other'
                for r in rows:
                    if r.has_key(c):
                        if not(r[c] == None):
                            r[c] = category_map[r[c]]
            else:
                raise DataValidationException("""Categorical column '""" +
                    c + """' has """ + str(len(category_counts[c].keys())) +
                    """ unique values which exceeds the limit of """ +
                    str(MAX_CATS), col=c)
    if not(allow_nones):
        field_fill = {}
        for c in schema.keys():
            field_fill[c] = 0
        for r in rows:
            for c in r.keys():
                if not(field_fill.has_key(c)):
                    field_fill[c] = 0
                if not(r[c] == None):
                    field_fill[c] = field_fill[c] + 1
        for (c,fill) in field_fill.items():
            if (fill == 0):
                raise DataValidationException("""Column '""" + c +
                    """' does not have any values""", col=c)


def summarize(predictions, col):
    """Basic summary for predictions results.

    Calculates a point estimate and an associated estimate of uncertainty for
    a single column from predictions results.

    For real columns, returns the mean and standard deviation. For count
    columns, returns the mean rounded to the nearest integer and standard
    deviation. For categorical and boolean columns, returns the modal value
    and the total probability of all values other than the mode.

    Arguments:
    predictions -- predictions results as a list of row dicts
    col -- the column to summarize

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    coltype = type(predictions[0][col])
    vals = [p[col] for p in predictions]
    cnt = len(vals)
    if coltype in (int, float):
        e = sum(vals) / float(cnt) # use the mean
        if cnt == 1:
            c = 0
        else:
            c = pow(sum([pow(v - e, 2) for v in vals]) / float(cnt - 1), 0.5)
        if coltype == int:
            return (int(round(e, 0)), c)
        else:
            return (e, c)
    elif coltype in (str, bool):
        e = max(vals, key=vals.count)
        c = 1 - (sum([1.0 for v in vals if v == e]) / float(cnt))
        return (e, c)
