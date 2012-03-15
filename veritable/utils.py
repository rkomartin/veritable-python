import time
import uuid
from math import floor
from random import shuffle
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse    
import csv
import re
import string
from .exceptions import *


_alphanumeric = re.compile("^[-_a-zA-Z0-9]+$")

def _check_id(id):
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
    """Waits for a running analysis to succeed or fail."""
    while a.state == 'running':
        time.sleep(poll)
        a.update()

def split_rows(rows, frac):
    """Splits a list of rows into two sets, sampling at random."""
    N = len(rows)
    inds = range(N)
    shuffle(inds)
    border_ind = int(floor(N * frac))
    train_dataset = [rows[i] for i in inds[0:border_ind]]
    test_dataset = [rows[i] for i in inds[border_ind:]]
    return train_dataset, test_dataset

def _validate_schema(schema):
    # Validate a schema
    valid_types = ['boolean', 'categorical', 'real', 'count']
    for k in schema.keys():
        if not isinstance(k, str):
            raise InvalidSchemaException()
        v = schema[k]
        if not ('type' in v.keys()):
            raise InvalidSchemaException("""Column '""" + k +
                """' does not have a 'type' specified. Please specify
                'type' as one of ['""" + "', '".join(valid_types) +
                """']""", col=k)
        if not v['type'] in valid_types:
            raise InvalidSchemaException("""Column '""" + k + """' type '""" +
                v['type'] + """' is not valid. Please specify 'type' as
                one of ['""" + "', '".join(valid_types) + """']""",
                col=k)

def validate_schema(schema):
    """Checks if an analysis schema is well-formed."""
    try:
        _validate_schema(schema)
    except:
        return False
    else:
        return True

def make_schema(schema_rule, headers=None, rows=None):
    """Makes an analysis schema from a schema rule."""
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
def write_csv(rows, filename, dialect=csv.excel):
    """Writes a list of row dicts to disk as .csv"""
    headers = set()
    for r in rows:
        headers = headers.union(r.keys())
    headers = list(headers)
    headers.sort()
    with open(filename,'w') as outFile:
        writer = csv.writer(outFile, dialect=dialect)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([('' if not(c in r)
                              else '' if r[c] == None
                              else str(r[c])) for c in headers])

# Dialects: csv.excel_tab, csv.excel
def read_csv(filename, id_col=None, dialect=None, na_vals=['']):
    """Reads a .csv from disk into a list of row dicts."""
    table = []
    with open(filename) as f:
        if dialect == None:
            dialect = csv.Sniffer().sniff(f.read(1024))
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
    """Validates a list of rows against an analysis schema."""
    return _validate(rows, schema, convert_types=convert_types,
        allow_nones=False, remove_nones=remove_nones,
        remove_invalids=remove_invalids, map_categories=map_categories,
        has_ids=True, assign_ids=assign_ids, allow_extra_fields=True,
        remove_extra_fields=remove_extra_fields)

def validate_predictions(predictions, schema, convert_types=False,
    remove_invalids=False, remove_extra_fields=False):
    """Validates a predictions request against an analysis schema."""
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
            if not('_id' in r):
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
                raise DataValidationException("""Row:'""" + str(i) +
                    """' Key:'_id' Value:'""" + str(r['_id']) +
                    """' is """ + str(type(r['_id'])) + """, not an ascii str""",
                    row=i, col='_id')
            else:
                try:
                    r['_id'].encode('utf-8').decode('ascii')
                except UnicodeDecodeError:
                    raise DataValidationException("""Row:'""" + str(i) +
                        """' Key:'_id' Value:'""" + str(r['_id']) +
                        """' is """ + str(type(r['_id'])) + """, not an ascii str""",
                        row=i, col='_id')
            try:
                _check_id(r['_id'])
            except InvalidIDException:
                raise DataValidationException("""Row:'""" + str(i) +
                        """' Key:'_id' Value:'""" + str(r['_id']) +
                        """' is not an alphanumeric/hyphen/underscore only string""",
                        row=i, col='_id')
            if r['_id'] in unique_ids:
                raise DataValidationException("""Row:'""" + str(i) +
                    """' Key:'_id' Value:'""" + str(r['_id']) +
                    """' is not unique, conflicts with Row:'""" +
                    str(unique_ids[r['_id']]) + """'""", row=i, col='_id')
            unique_ids[r['_id']] = i
        elif '_id' in r:
            if remove_extra_fields:
                r.pop('_id')
            else:
                raise DataValidationException("""Row:'""" + str(i) +
                    """' Key:'_id' should not be included""", row=i, col='_id')
        for c in list(r.keys()):
            if not(c == '_id'):
                if not(c in schema):
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
                            if not(c in category_counts):
                                category_counts[c] = {}
                            if not(r[c] in category_counts[c]):
                                category_counts[c][r[c]] = 0
                            category_counts[c][r[c]] = category_counts[c][r[c]] + 1
    MAX_CATS = 256
    for c in category_counts.keys():
        cats = list(category_counts[c].keys())
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
                    if c in r:
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
                if not(c in field_fill):
                    field_fill[c] = 0
                if not(r[c] == None):
                    field_fill[c] = field_fill[c] + 1
        for (c,fill) in field_fill.items():
            if (fill == 0):
                raise DataValidationException("""Column '""" + c +
                    """' does not have any values""", col=c)


def summarize(predictions, col):
    """Calculates a point estimate and an associated estimate of uncertainty
        for a single column from predictions results.

        For real columns, this returns the mean and standard deviation. For
        count columns, this returns the mean rounded to the nearest integer
        and standard deviation. For categorical and boolean columns, this is
        the mode and the probability that another value was predicted."""
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
