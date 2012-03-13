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
        if not isinstance(k, basestring):
            raise InvalidSchemaException()
        v = schema[k]
        if not ('type' in v.keys()):
            raise InvalidSchemaException("Column '"+k+"' does not have a 'type' specified. Please specify 'type' as one of ['"+string.join(valid_types,"', '")+"']",col=k)
        if not v['type'] in valid_types:
            raise InvalidSchemaException("Column '"+k+"' type '"+v['type']+"' is not valid. Please specify 'type' as one of ['"+string.join(valid_types,"', '")+"']",col=k)

def validate_schema(schema):
    """Checks if an analysis schema is well-formed."""
    try:
        _validate_schema(schema)
    except:
        return False
    else:
        return True

def make_schema(schema_rule,headers=None,rows=None):
    """Makes an analysis schema from a schema rule."""
    if ((headers == None) & (rows == None)):
        raise Exception("Either headers or rows must be provided")
    if headers == None:
        headerSet = {}
        for r in rows:
            for c in r.keys():
                headerSet[c] = True
        headers = headerSet.keys()
    schema = {}
    for c in headers:
        for (r,t) in schema_rule:
            if re.match(r,c):
                schema[c] = t
                break
    return schema


# Dialects: csv.excel_tab, csv.excel
def write_csv(rows,filename,dialect=csv.excel):
    """Writes a list of row dicts to disk as .csv"""
    headers = {}
    for r in rows:
        for c in r.keys():
            headers[c] = True
    headers = headers.keys()
    headers.sort()
    with open(filename,'wb') as outFile:
        writer = csv.writer(outFile,dialect=dialect)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([('' if not(r.has_key(c))
                              else '' if r[c] == None
                              else str(r[c])) for c in headers])

# Dialects: csv.excel_tab, csv.excel
def read_csv(filename, id_col=None, dialect=None, na_vals=['']):
    """Reads a .csv from disk into a list of row dicts."""
    table = []
    with open(filename) as cacheFile:
        if dialect == None:
            dialect = csv.Sniffer().sniff(cacheFile.read(1024))
        cacheFile.seek(0)
        reader = csv.reader(cacheFile, dialect)
        header = [h.strip() for h in reader.next()]
        if '_id' in header:
            id_col = '_id'
        rowCount = 0
        for row in reader:
            rowCount = rowCount + 1
            rowSet = {}
            for i in range(min(len(header),len(row))):
                val = row[i].strip()
                if not(val in na_vals):
                    if(header[i] == id_col):
                        rowSet['_id'] = val
                    else:
                        rowSet[header[i]] = val
                else:
                    if header[i] == id_col:
                        raise VeritableException("Missing id for row" + str(i))
            if id_col == None:
                rowSet['_id'] = str(rowCount)
            table.append(rowSet)
    return table;

def validate_data(rows,schema,convert_types=False,remove_nones=False,remove_invalids=False,map_categories=False,assign_ids=False,remove_extra_fields=False):
    """Validates a list of rows against an analysis schema."""
    return _validate(rows,schema,convert_types=convert_types,allow_nones=False,remove_nones=remove_nones,remove_invalids=remove_invalids,map_categories=map_categories,has_ids=True,assign_ids=assign_ids,allow_extra_fields=True,remove_extra_fields=remove_extra_fields)

def validate_predictions(predictions,schema,convert_types=False,remove_invalids=False,remove_extra_fields=False):
    """Validates a predictions request against an analysis schema."""
    return _validate(predictions,schema,convert_types=convert_types,allow_nones=True,remove_nones=False,remove_invalids=remove_invalids,map_categories=False,has_ids=False,assign_ids=False,allow_extra_fields=False,remove_extra_fields=remove_extra_fields)

def _validate(rows,schema,convert_types,allow_nones,remove_nones,remove_invalids,map_categories,has_ids,assign_ids,allow_extra_fields,remove_extra_fields):
    # Validates data against an analysis schema
    _validate_schema(schema)
    prevIDs = {}
    catCounts = {}
    for i in range(len(rows)):
        r = rows[i]
        if assign_ids:
            r['_id'] = str(i)
        elif has_ids:
            if not(r.has_key('_id')):
                raise DataValidationException("Row:'"+str(i)+"' is missing Key:'_id'",row=i,col='_id')
            if convert_types:
                r['_id'] = str(r['_id'])
            if not (type(r['_id']) == str):
                raise DataValidationException("Row:'"+str(i)+"' Key:'_id' Value:'"+str(r['_id'])+"' is "+str(type(r['_id']))+", not a str",row=i,col='_id')
            if prevIDs.has_key(r['_id']):
                raise DataValidationException("Row:'"+str(i)+"' Key:'_id' Value:'"+str(r['_id'])+"' is not unique, conflicts with Row:'"+str(prevIDs[r['_id']])+"'",row=i,col='_id')
            prevIDs[r['_id']] = i
        elif r.has_key('_id'):
            if remove_extra_fields:
                r.pop('_id')
            else:
                raise DataValidationException("Row:'"+str(i)+"' Key:'_id' should not be included",row=i,col='_id')
        for c in r.keys():
            if not(c == '_id'):
                if not(schema.has_key(c)):
                    if remove_extra_fields:
                        r.pop(c)
                    else:
                        if not(allow_extra_fields):
                            raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' is not defined in schema",row=i,col=c)                    
                elif r[c] == None:
                    if remove_nones:
                        r.pop(c)
                    else:
                        if not(allow_nones):
                            raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' should be removed because it has value None",row=i,col=c)                    
                else:
                    ctype = schema[c]['type']
                    if(ctype == 'count'):
                        if convert_types:                            
                            try:
                                r[c] = int(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not(type(r[c]) == int):
                                raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not an int",row=i,col=c)                            
                    elif(ctype == 'real'):
                        if convert_types:                            
                            try:
                                r[c] = float(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not(type(r[c]) == float):
                                raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a float",row=i,col=c)
                    elif(ctype == 'boolean'):
                        if convert_types:                            
                            try:
                                r[c] = True if str(r[c]).strip().lower() in ['true','yes','y'] else False if str(r[c]).strip().lower() in ['false','no','n'] else bool(int(r[c]))
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not(type(r[c]) == bool):
                                raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a bool",row=i,col=c)   
                    elif(ctype == 'categorical'):
                        if convert_types:                            
                            try:
                                r[c] = str(r[c])
                            except:
                                if remove_invalids:
                                    r[c] = None
                        if r[c] == None:
                            r.pop(c)
                        else:
                            if not(type(r[c]) == str):
                                raise DataValidationException("Row:'"+str(i)+"' Key:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a str",row=i,col=c)
                            if not(catCounts.has_key(c)):
                                catCounts[c] = {}
                            if not(catCounts[c].has_key(r[c])):
                                catCounts[c][r[c]] = 0
                            catCounts[c][r[c]] = catCounts[c][r[c]] + 1
    maxCats = 256
    for c in catCounts.keys():
        cats = catCounts[c].keys()
        if (len(cats) > maxCats):
            if map_categories:
                cats.sort(key=lambda cat: catCounts[c][cat])
                cats.reverse()
                catMap = {}
                for j in range(len(cats)):
                    if j < (maxCats - 1):
                        catMap[cats[j]] = cats[j]
                    else:
                        catMap[cats[j]] = 'Other'
                for r in rows:
                    if r.has_key(c):
                        if not(r[c] == None):
                            r[c] = catMap[r[c]]
            else:
                raise DataValidationException("Categorical column '"+c+"' has "+str(len(catCounts[c].keys()))+" unique values which exceeds the limit of "+str(maxCats),col=c)
    if not(allow_nones):
        fieldFill = {}
        for c in schema.keys():
            fieldFill[c] = 0
        for r in rows:
            for c in r.keys():
                if not(fieldFill.has_key(c)):
                    fieldFill[c] = 0
                if not(r[c] == None):
                    fieldFill[c] = fieldFill[c]+1
        for (c,fill) in fieldFill.items():
            if (fill == 0):
                raise DataValidationException("Column '"+c+"' does not have any values",col=c)



def summarize(predictions, col):
    """Gives a basic summary of predictions results."""
    ctype = type(predictions[0][col])
    vals = [p[col] for p in predictions]
    cnt = len(vals)
    if ctype in (int,float):
        e = sum(vals) / float(cnt)
        c = 0 if cnt == 1 else pow(sum([pow(v-e,2) for v in vals])/float(cnt-1),0.5)
        if ctype == int:
            return (int(round(e,0)),c)
        else:
            return (e,c)
    elif ctype in (str,bool):
        e = max(vals, key=vals.count)
        c = 1 - (sum([1.0 for v in vals if v == e]) / float(cnt))
        return (e,c)
