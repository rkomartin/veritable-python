import time
import uuid
from math import floor
from random import shuffle
from urlparse import urlparse
import csv
import re
from collections import Counter
from .exceptions import *


def _make_table_id():
    return uuid.uuid4().hex

def _make_analysis_id():
    return uuid.uuid4().hex

def _format_url(*args):
    return "/".join([x.rstrip("/").lstrip("/") for x in args])

def _url_has_scheme(url):
	return urlparse(url)[0] is not ""

def wait_for_analysis(a, poll=2):
	while a.status() == "running":
		time.sleep(poll)

def split_rows(rows, frac):
	N = len(rows)
	inds = range(N)
	shuffle(inds)
	border_ind = int(floor(N * frac))
	train_dataset = [rows[i] for i in inds[0:border_ind]]
	test_dataset = [rows[i] for i in inds[border_ind:]]
	return train_dataset, test_dataset

def _validate_schema(schema):
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

def validate_schema(schema):
	try:
		_validate_schema(schema)
	except:
		return False
	else:
		return True

def make_schema(schemaRule,headers=None,rows=None):
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
        for (r,t) in schemaRule:
            if re.match(r,c):
                schema[c] = t
                break
    return schema


# Dialects: csv.excel_tab, csv.excel
def write_csv(rows,fileName,dialect=csv.excel):
    headers = {}
    for r in rows:
        for c in r.keys():
            headers[c] = True
    headers = headers.keys()
    headers.sort()
    with open(fileName,'wb') as outFile:
        writer = csv.writer(outFile,dialect=dialect)
        writer.writerow(headers)
        for r in rows:
            writer.writerow([('' if not(r.has_key(c))
                              else '' if r[c] == None
                              else str(r[c])) for c in headers])

# Dialects: csv.excel_tab, csv.excel
def read_csv(fileName,idCol=None,dialect=None):
    table = []
    with open(fileName) as cacheFile:
        if dialect == None:
            dialect = csv.Sniffer().sniff(cacheFile.read(1024))
        cacheFile.seek(0)
        reader = csv.reader(cacheFile, dialect)
        header = [h.strip() for h in reader.next()]
        if '_id' in header:
            idCol = '_id'
        rowCount = 0
        for row in reader:
            rowCount = rowCount + 1
            rowSet = {}
            for i in range(min(len(header),len(row))):
                val = row[i].strip()
                if(header[i] == idCol):
                    rowSet['_id'] = val
                else:
                    rowSet[header[i]] = None if val == '' else val
            if idCol == None:
                rowSet['_id'] = str(rowCount)
            table.append(rowSet)
    return table;

def validate(rows,schema,convertTypes=False,nullInvalids=False,mapCategories=False,assignIDs=False,removeExtraFields=False):
    for c in schema.keys():
        if not(schema[c].has_key('type')):
            raise DataValidationException("Field '"+c+"' does not have a 'type' specified. Please specify 'type' as one of ['count','real','boolean','categorical']",field=c)            
        ctype = schema[c]['type']
        if not(ctype in ['count','real','boolean','categorical']):
            raise DataValidationException("Invalid type '"+ctype+"' for field '"+c+"'. Must be one of ['count','real','boolean','categorical']",field=c)
    prevIDs = {}
    catCounts = {}
    for i in range(len(rows)):
        r = rows[i]
        if assignIDs:
            r['_id'] = str(i)
        else:
            if not(r.has_key('_id')):
                raise DataValidationException("Row:'"+str(i)+"' is missing Field:'_id'",row=i,field='_id')
            if convertTypes:
                r['_id'] = str(r['_id'])
            if not (type(r['_id']) == str):
                raise DataValidationException("Row:'"+str(i)+"' Field:'_id' Value:'"+str(r['_id'])+"' is "+str(type(r['_id']))+", not a str",row=i,field='_id')
            if prevIDs.has_key(r['_id']):
                raise DataValidationException("Row:'"+str(i)+"' Field:'_id' Value:'"+str(r['_id'])+"' is not unique, conflicts with Row:'"+str(prevIDs[r['_id']])+"'",row=i,field='_id')
            prevIDs[r['_id']] = i
        for c in r.keys():
            if not(c == '_id'):
                if not(schema.has_key(c)):
                    if removeExtraFields:
                        r.pop(c)
                    else:
                        raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' is not defined in schema",row=i,field=c)                    
                else:
                    if not(r[c] == None):
                        ctype = schema[c]['type']
                        if(ctype == 'count'):
                            if convertTypes:                            
                                try:
                                    r[c] = int(r[c])
                                except:
                                    if nullInvalids:
                                        r[c] = None
                            if not(r[c] == None):
                                if not(type(r[c]) == int):
                                    raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not an int",row=i,field=c)
                        elif(ctype == 'real'):
                            if convertTypes:                            
                                try:
                                    r[c] = float(r[c])
                                except:
                                    if nullInvalids:
                                        r[c] = None
                            if not(r[c] == None):
                                if not(type(r[c]) == float):
                                    raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a float",row=i,field=c)
                        elif(ctype == 'boolean'):
                            if convertTypes:                            
                                try:
                                    r[c] = True if str(r[c]).strip().lower() in ['true','yes','y'] else False if str(r[c]).strip().lower() in ['false','no','n'] else bool(int(r[c]))
                                except:
                                    if nullInvalids:
                                        r[c] = None
                            if not(r[c] == None):
                                if not(type(r[c]) == bool):
                                    raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a bool",row=i,field=c)   
                        elif(ctype == 'categorical'):
                            if convertTypes:                            
                                try:
                                    r[c] = str(r[c])
                                except:
                                    if nullInvalids:
                                        r[c] = None
                            if not(r[c] == None):
                                if not(type(r[c]) == str):
                                    raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a str",row=i,field=c)
                                if not(catCounts.has_key(c)):
                                    catCounts[c] = {}
                                if not(catCounts[c].has_key(r[c])):
                                    catCounts[c][r[c]] = 0
                                catCounts[c][r[c]] = catCounts[c][r[c]] + 1
    maxCats = 256
    for c in catCounts.keys():
        cats = catCounts[c].keys()
        if (len(cats) > maxCats):
            if mapCategories:
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
                raise DataValidationException("Categorical field '"+c+"' has "+str(len(catCounts[c].keys()))+" unique values which exceeds the limit of "+str(maxCats),field=c)
    fieldFill = {}
    for r in rows:
        for c in r.keys():
            if not(fieldFill.has_key(c)):
                fieldFill[c] = 0
            if not(r[c] == None):
                fieldFill[c] = fieldFill[c]+1
    for (c,fill) in fieldFill.items():
        if fill == 0:
            raise DataValidationException("Field '"+c+"' does not have non-empty values",field=c)


def summarize(predictions, colName):
    ctype = type(predictions[0][colName])
    if ctype in (int,float):
        e = sum([p[colName] for p in predictions]) / float(len(predictions))
        c = 0 if len(predictions) == 1 else pow(sum([pow(p[colName]-e,2) for p in predictions])/float(len(predictions)-1),0.5)
        if ctype == int:
            return (int(round(e,0)),c)
        else:
            return (e,c)
    elif ctype in (str,bool):
        e,c = Counter([p[colName] for p in predictions]).most_common(1)[0]
        c = c / float(len(predictions))
        return (e,c)
