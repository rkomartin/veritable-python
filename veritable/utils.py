"""Utility functions for working with veritable-python.

See also: https://dev.priorknowledge.com/docs/client/python

"""

import uuid
from math import floor, ceil, log, isnan, isinf
from random import shuffle
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus
import csv
import re
from .exceptions import VeritableError


_alphanumeric = re.compile("^[-_a-zA-Z0-9]+$")
COUNT_LIMIT = 100000


def _handle_unicode_id(id):
    try:
        if isinstance(id, basestring):
            try:
                id = str(id)
            except:
                pass
    except:
        pass
    return id
    
def _check_id(id):
    try:
        if isinstance(id, basestring):
            id = str(id)
    except:
        pass
    if not isinstance(id, str) or _alphanumeric.match(id) is None or id[-1] == "\n":
        try:
            str(id)
        except:
            raise VeritableError("Specified id is invalid: " \
            "alphanumeric, underscore, and hyphen only!")
        else:
            raise VeritableError("Specified id {0} is invalid: " \
            "alphanumeric, underscore, and hyphen " \
            "only!".format(str(id)))
    if id[0] == '_':
        raise VeritableError("Specified id is invalid: may not begin with an underscore!")

def _is_str(s):
    try:
        if isinstance(s, basestring):
            s = str(s)
    except:
        pass
    return isinstance(s, str)


def _make_table_id():
    # Autogenerate id
    return uuid.uuid4().hex


def _make_analysis_id():
    # Autogenerate id
    return uuid.uuid4().hex


def _url_has_scheme(url):
    # Check if a URL includes a scheme
    return urlparse(url)[0] is not ""


def _format_url(path, noquote=[]):
    # Joins the path elements in path (a collection) with "/"
    # If the index of a path element is not in noquote, the element
    # will be quoted using urllib.quote_plus
    for i in range(len(path)):
        if i == 0:
            if i in noquote:
                path[i] = path[i].rstrip("/")
            else:
                path[i] = quote_plus(path[i].rstrip("/"))
        else:
            if i in noquote:
                path[i] = path[i].rstrip("/").lstrip("/")
            else:
                path[i] = quote_plus(path[i].rstrip("/").lstrip("/"))
    return "/".join(path)


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
    # Checks whether a schema is well formed and raises a
    # VeritableError if not.
    valid_types = ['boolean', 'categorical', 'real', 'count']
    for k in schema.keys():
        if not isinstance(k, str):
            try:
                isinstance(k, basestring)
            except:
                try:
                    str(k)
                except:
                    raise VeritableError("Invalid schema specification.")
                else:
                    raise VeritableError("Invalid schema specification: " \
                    "{0} is not a valid string column name.".format(str(k)))
        if k[0] == "_":
            raise VeritableError("Invalid schema specification: " \
                "Column name {0} begins with an underscore.".format(k))
        if '.' in k:
            raise VeritableError("Invalid schema specification: " \
                "Column name {0} contains invalid character: .".format(k))
        if '$' in k:
            raise VeritableError("Invalid schema specification: " \
                "Column name {0} contains invalid character: $".format(k))
        v = schema[k]
        if not ('type' in v.keys()):
            raise VeritableError("Invalid schema specification: " \
            "Column '{0}' does not have a 'type' specified. Please " \
            "specify 'type' as one of ['{1}']".format(k,
                "', '".join(valid_types), col=k))
        if not v['type'] in valid_types:
            raise VeritableError("Invalid schema specification: " \
            "Column '{0}' type '{1}' is not valid. Please specify " \
            "'type' as one of ['{2}']".format(k, v['type'],
                "', '".join(valid_types), col=k))


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
        [['a_regex_to_match', {'type': 'continuous'}],
         [lambda h, v: <some boolean return value>,
          {'type': 'count'}], ...]
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
        raise VeritableError("Either headers or rows must be provided!")
    if headers is None:
        headers = set()
        for r in rows:
            headers = headers.union(r.keys())
    schema = {}
    for i in range(len(schema_rule)):
        try:
            schema_rule[i][0] = re.compile(schema_rule[i][0])
        except:
            pass
    for c in headers:
        for (r, t) in schema_rule:
            try:
                if r.match(c):
                    schema[c] = t
                    break
            except AttributeError:
                if rows is None:
                    if r(c, None):
                        schema[c] = t
                        break
                else:
                    v = []
                    for row in rows:
                        if c in row:
                            v.append(row[c])
                    if r(c, v):
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
    with open(filename, 'w') as out_file:
        writer = csv.writer(out_file, dialect=dialect)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([(na_val if not c in r
                              else na_val if r[c] is None
                              else str(r[c])) for c in headers])


# Dialects: csv.excel_tab, csv.excel
def read_csv(filename, id_col=None, dialect=csv.excel, na_vals=['']):
    """Reads a .csv from disk into a list of row dicts.

    Returns a list of dicts representing the rows in the .csv file.

    Does not support .csvs that contain Unicode values.

    Arguments:
    filename -- the .csv file to read from
    id_col -- the column, if any, containing unique row ids (default: None)
        If None, the rows will be numbered sequentially; otherwise, this
        column will be renamed to '_id' (as required by the row upload
        functions). If id_col is specified, but ids are missing for some rows,
        then a VeritableError will be raised.
    dialect -- a subclass of csv.Dialect to use in reading the .csv file
        (default: csv.excel)
    na_vals -- a list of values to treat as NA (default: ['']) Each row dict
        will contain only those columns in which these values do not occur.

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    table = []
    with open(filename, "rU") as f:
        f.seek(0)
        reader = csv.reader(f, dialect)
        header = [h.strip() for h in next(reader)]
        if '_id' in header:
            id_col = '_id'
        if id_col is None:
            rid = 0
        for row in reader:
            r = {}
            for i in range(min(len(header), len(row))):
                val = row[i].strip()
                if not val in na_vals:
                    if(header[i] == id_col):
                        r['_id'] = val
                    else:
                        r[header[i]] = val
                else:
                    if header[i] == id_col:
                        raise VeritableError("Missing id for row" + str(i))
            if id_col is None:
                rid = rid + 1
                r['_id'] = str(rid)
            table.append(r)
    return table

def clean_data(rows, schema, convert_types=True, remove_nones=True,
    remove_invalids=True, reduce_categories=True, assign_ids=False,
    remove_extra_fields=False, rename_columns=False):
    """Cleans up a list of row dicts in accordance with an analysis schema.

    Raises a VeritableError containing further details if the data
    does not validate against the schema.

    Note: This function mutates its rows argument. If clean_data raises an
    exception, values in some rows may be converted while others are left in
    their original state.

    Arguments:
    rows -- the list of row dicts to clean up
    schema -- an analysis schema specifying the types of the columns appearing
        in the rows being cleaned
    convert_types -- controls whether clean_data will attempt to convert
        cells in a column to be of the correct type (default: True)
    remove_nones -- controls whether clean_data will automatically remove
        cells containing the value None (default: True)
    remove_invalids -- controls whether clean_data will automatically
        remove cells that are invalid for a given column (default: True)
    reduce_categories -- controls whether clean_data will automatically
        reduce the number of categories in categorical columns with too many
        categories (default: True) If True, the largest categories in a
        column will be preserved, up to the allowable limit, and the other
        categories will be binned as "Other".
    assign_ids -- controls whether clean_data will automatically assign new
        ids to the rows (default: False) If True, rows will be numbered
        sequentially in the column '_id'. If a string, sequential ids will be
        put into column assign_ids rather than '_id'. If the rows have an
        existing column of the same name, remove_extra_fields must also be set
        to True to avoid raising a VeritableError.
    remove_extra_fields -- controls whether clean_data will automatically
        remove columns that are not contained in the schema (default: False)
        If assign_ids is True (default), will also remove the id column ('_id'
        or as specified by assign_ids).
    rename_columns -- a list of two-valued lists containing ['col_to_rename',
        'new_name'], or False (default), in which case column names will not
        be changed. Will silently succeed if asked to rename columns not
        present in the dataset.

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(rows, schema, convert_types=convert_types,
        allow_nones=False, remove_nones=remove_nones,
        remove_invalids=remove_invalids, reduce_categories=reduce_categories,
        has_ids='_id', assign_ids=assign_ids, allow_extra_fields=True,
        remove_extra_fields=remove_extra_fields, allow_empty_columns=False,
        rename_columns=False)

def validate_data(rows, schema):
    """Validates a list of row dicts against an analysis schema.

    Raises a DataValidationException containing further details if the data
    does not validate against the schema.

    Arguments:
    rows -- the list of row dicts to validate
    schema -- an analysis schema specifying the types of the columns appearing
        in the rows being validated

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(rows, schema, convert_types=False,
        allow_nones=False, remove_nones=False,
        remove_invalids=False, reduce_categories=False,
        has_ids='_id', assign_ids=False, allow_extra_fields=True,
        remove_extra_fields=False, allow_empty_columns=False,
        rename_columns=False)

def clean_predictions(predictions, schema, convert_types=True, assign_ids=True,
    remove_invalids=True, remove_extra_fields=True, rename_columns=[['_id', '_request_id']]):
    """Cleans up a predictions request in accordance with an analysis schema.

    Raises a DataValidationException containing further details if the
    predictions request does not validate against the schema.

    Automatically converts an '_id' column, if present, to '_request_id',
    otherwise assigns new a unique '_request_id' to each row.

    Note: This function mutates its predictions argument. If clean_predictions
    raises an exception, values in some columns may be converted while others
    are left in their original state.

    Arguments:
    predictions -- the predictions request to clean up
    schema -- an analysis schema specifying the types of the columns appearing
        in the dataset
    convert_types -- controls whether clean_predictions will attempt to
        convert fixed cells in a column to be of the correct type
        (default: True)
    assign_ids -- controls whether clean_predictions will automatically assign new
        ids to the rows (default: True) If True, rows will be numbered
        sequentially in the column '_request_id'. If a string, sequential ids will be
        put into column assign_ids rather than '_request_id'. If the rows have an
        existing column of the same name, remove_extra_fields must also be set
        to True to avoid raising a VeritableError.
    remove_invalids -- controls whether clean_predictions will automatically
        remove fixed cells that are invalid for a given column (default: True)
    remove_extra_fields -- controls whether clean_predictions will
        automatically remove columns that are not contained in the schema
        (default: True)
    rename_columns -- a list of two-valued lists containing ['col_to_rename',
        'new_name'], or False, in which case column names will not
        be changed. Will silently succeed if asked to rename columns not
        present in the dataset. Default is [['_id', '_request_id']]

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(predictions, schema, convert_types=convert_types,
        allow_nones=True, remove_nones=False, remove_invalids=remove_invalids,
        reduce_categories=False, has_ids='_request_id', assign_ids=assign_ids,
        allow_extra_fields=False, remove_extra_fields=remove_extra_fields,
        allow_empty_columns=True, rename_columns=rename_columns)

def validate_predictions(predictions, schema):
    """Validates a predictions request against an analysis schema.

    Raises a VeritableError containing further details if the request
    does not validate against the schema.

    Arguments:
    predictions -- the predictions request to validate
    schema -- an analysis schema specifying the types of the columns appearing
        in the predictions request being validated

    See also: https://dev.priorknowledge.com/docs/client/python

    """
    return _validate(predictions, schema, convert_types=False,
        allow_nones=True, remove_nones=False, remove_invalids=False,
        reduce_categories=False, has_ids='_request_id', assign_ids=False,
        allow_extra_fields=False, remove_extra_fields=False,
        allow_empty_columns=True, rename_columns=False)


def _validate(rows, schema, convert_types, allow_nones, remove_nones,
    remove_invalids, reduce_categories, has_ids, assign_ids,
    allow_extra_fields, remove_extra_fields, allow_empty_columns,
    rename_columns):
    # First check that the schema is well formed
    _validate_schema(schema)

    # figure out which column holds the unique id
    if has_ids or assign_ids:
        if isinstance(has_ids, str) and isinstance(assign_ids, str):
            if not has_ids == assign_ids:
                raise VeritableError("Can't assign new row ids to column " \
                    "'{0}' when ids are expected in column " \
                    "'{1}'.".format(assign_ids, has_ids))
        if isinstance(assign_ids, str):
            id_col = assign_ids
        elif isinstance(has_ids, str):
            id_col = has_ids
        else:
            id_col = '_id'

    # unique_ids stores the row numbers of each unique id so that if an id is
    #   repeated we can alert the user appropriately
    unique_ids = {}

    # field_fill keeps track of the density of all fields present
    field_fill = {}
    for c in schema.keys():
        if c != id_col:
            field_fill[c] = 0

    # category_counts stores the number of categories in each categorical
    #   column
    category_counts = {}

    # values which will be converted to True and False in boolean columns
    #   if convert_types
    TRUE_STRINGS = ['true', 't', 'yes', 'y']
    FALSE_STRINGS = ['false', 'f', 'no', 'n']

    # be careful before changing the order of any of this logic - the point is
    #   to map through the rows only once
    for i in range(len(rows)):
        r = rows[i]
        if rename_columns: # first apply the column renaming rules, if any
            if not isinstance(rename_columns, list):
                raise VeritableError("Must supply column renaming rules as " \
                    "a list of lists.")
            for rule in rename_columns:
                if not isinstance(rule, list):
                    raise VeritableError("Must supply column renaming rules "\
                        "as a list of lists.")
                try:
                    r[rule[1]] = r[rule[0]]
                    del r[rule[0]]
                except KeyError:
                    pass
        if assign_ids:  # number the rows sequentially
            r[id_col] = str(i)
        elif has_ids:   # we expect an id_col column
            if not id_col in r:
                raise VeritableError("Row: {0} is missing " \
                "Key:'{1}'".format(str(i), id_col), row=i, col=id_col)
            if convert_types:   # attempt to convert id_col to string
                try:
                    r[id_col] = str(r[id_col])
                except UnicodeDecodeError:  # catch and use str.encode
                    raise VeritableError("Row:'{0}' Key:'{1}' Value:'{2}' " \
                    "is {3}, not a str".format(str(i), id_col,
                        r[id_col].encode('utf-8'), str(type(r[id_col]))),
                        row=i, col=id_col)
            if not isinstance(r[id_col], str):  # invalid type for id_col
                    try:
                        str(r[id_col])
                    except UnicodeEncodeError:  # ensure we work in 2.7 and 3
                        raise VeritableError("Row:'{0}' Key:'{1}' is {2}, " \
                        "not an ascii str.".format(str(i), id_col, 
                            str(type(r[id_col]))), row=i, col=id_col)
                    else:
                        raise VeritableError("Row:'{0}' Key:'{1}' " \
                        "Value:'{1}' is {2}, not an ascii " \
                        "str.".format(str(i), id_col, r[id_col],
                            str(type(r[id_col]))), row=i, col=id_col)
            else:
                try:
                    r[id_col].encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    raise VeritableError("Row:'{0}' Key:'{1}' Value:'{2}' " \
                    "is {3}, not an ascii str.".format(str(i), id_col,
                        str(r[id_col]), str(type(r[id_col]))), row=i,
                        col=id_col)
            try:  # make sure id_col is alphanumeric
                _check_id(r[id_col])
            except VeritableError:
                raise VeritableError("Row:'{0}' Key:'{1}' Value:'{2}' must " \
                "contain only alphanumerics, underscores, and " \
                "hyphens".format(str(i), id_col, str(r[id_col])), row=i,
                col=id_col)
            if r[id_col] in unique_ids:
                raise VeritableError("Row:'{0}' Key:'{1}' Value:'{2}' is " \
                "not unique, conflicts with Row:'{3}'".format(str(i), id_col,
                    str(r[id_col]), str(unique_ids[r[id_col]])), row=i,
                    col=id_col)
            unique_ids[r[id_col]] = i
        elif id_col in r:  # no ids, no autoid, but id_col column
            if remove_extra_fields:  # just remove it
                r.pop(id_col)
            else:
                raise VeritableError("Row:'{0}' Key:{1} should not be " \
                "included".format(str(i), id_col), row=i, col=id_col)
        for c in list(r.keys()):
            if c != id_col:
                if not c in schema:  # keys missing from schema
                    if remove_extra_fields:  # remove it
                        r.pop(c)
                    else:
                        if not allow_extra_fields:  # or silently allow
                            raise VeritableError("Row:'{0}' Key: '{1}' is "\
                            "not defined in schema".format(str(i), c), row=i,
                                col=c)
                elif r[c] is None:  # None values
                    if remove_nones:  # remove
                        r.pop(c)
                    else:
                        if not allow_nones:  # or silently allow
                            raise VeritableError("Row:'{0}' Key:'{1}' " \
                            "should be removed because it has value " \
                            "None".format(str(i), c), row=i, col=c)
                else:  # keys present in schema
                    coltype = schema[c]['type']  # check the column type
                    if coltype == 'count':
                        if convert_types:  # try converting to int
                            try:
                                r[c] = int(r[c])
                            except:
                                if remove_invalids:  # flag for removal
                                    r[c] = None
                        if r[c] is None:  # remove flagged values
                            r.pop(c)
                        elif remove_invalids and isinstance(r[c], int) and (r[c] < 0 or r[c] > COUNT_LIMIT):
                            r.pop(c)
                        else:
                            if not isinstance(r[c], int) or not r[c] >= 0 or not r[c] <= COUNT_LIMIT:  # catch invalids
                                raise VeritableError("Row:'{0}' Key:'{1}' " \
                                "Value:'{2}' is {3}, not an int " \
                                "between 0 and {4}".format(str(i), c, str(r[c]),
                                    str(type(r[c])),COUNT_LIMIT), row=i, col=c)
                    elif coltype == 'real':
                        if convert_types:  # try converting to float
                            try:
                                r[c] = float(r[c])
                                if isnan(r[c]) or isinf(r[c]):
                                    raise VeritableError("Invalid NaN or Inf")
                            except:
                                if remove_invalids:  # flag for removal
                                    r[c] = None
                        if r[c] is None:  # remove flagged values
                            r.pop(c)
                        else:
                            if (not isinstance(r[c], float)) or isnan(r[c]) or isinf(r[c]):  # catch invalids
                                raise VeritableError("Row:'{0}' Key: '{1}' " \
                                "Value: '{2}' is {3}, not a " \
                                "valid float".format(str(i), c, str(r[c]),
                                    str(type(r[c]))), row=i, col=c)

                    elif coltype == 'boolean':
                        if convert_types:  # try converting to bool
                            lc = str(r[c]).strip().lower()
                            try:
                                if lc in TRUE_STRINGS:
                                    r[c] = True
                                elif lc in FALSE_STRINGS:
                                    r[c] = False
                                else:
                                    r[c] = bool(int(r[c]))
                            except:
                                if remove_invalids:  # flag for removal
                                    r[c] = None
                        if r[c] is None:  # remove flagged values
                            r.pop(c)
                        else:
                            if not isinstance(r[c], bool):  # catch invalids
                                raise VeritableError("Row:'{0}' Key:'{1}' " \
                                "Value:'{2}' is {3}, not a " \
                                "bool".format(str(i), c, str(r[c]),
                                    str(type(r[c]))), row=i, col=c)
                    elif coltype == 'categorical':
                        if convert_types:  # try converting to str
                            try:
                                r[c] = str(r[c])
                            except:
                                if remove_invalids:  # flag for removal
                                    r[c] = None
                        if r[c] is None:  # remove flagged values
                            r.pop(c)
                        else:
                            if not isinstance(r[c], str):  # catch invalids
                                raise VeritableError("Row:'{0}' Key:'{1}' " \
                                "Value:'{2}' is {3}, not a " \
                                "str".format(str(i), c, str(r[c]),
                                    str(type(r[c]))), row=i, col=c)
                            if not c in category_counts:  # increment count
                                category_counts[c] = {}
                            if not r[c] in category_counts[c]:
                                category_counts[c][r[c]] = 0
                            category_counts[c][r[c]] += 1
                if not c in field_fill and not remove_extra_fields:
                    field_fill[c] = 0
                if c in r and r[c] is not None:
                    field_fill[c] = field_fill[c] + 1

    MAX_CATS = 256
    for c in category_counts.keys():
        cats = list(category_counts[c].keys())
        if len(cats) > MAX_CATS:  # too many categories
            if reduce_categories:  # keep the largest MAX_CATS - 1
                cats.sort(key=lambda cat: category_counts[c][cat])
                cats.reverse()
                category_map = {}
                for j in range(len(cats)):
                    if j < (MAX_CATS - 1):
                        category_map[cats[j]] = cats[j]
                    else:
                        category_map[cats[j]] = 'Other'  # bin the rest
                for r in rows:
                    if c in r:
                        if r[c] is not None:
                            r[c] = category_map[r[c]]  # convert the values
            else:
                raise VeritableError("Categorical column '{0}' has {1} " \
                "unique values which exceeds the limit of {2}.".format(c,
                    str(len(category_counts[c].keys())), str(MAX_CATS)),
                    col=c)
    if not allow_empty_columns:
        for (c, fill) in field_fill.items():
            if fill == 0:
                raise VeritableError("Column '{0}' does not have any " \
                "values".format(c), col=c)






