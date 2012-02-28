import time
import uuid
from math import floor, ceil, log
from random import shuffle
from urlparse import urlparse
import csv
import re
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
	while a.state() == "running":
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

def make_schema(schema_rule,headers=None,rows=None):
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
def read_csv(filename,id_col=None,dialect=None):
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
                if not(val == ''):
                    if(header[i] == id_col):
                        rowSet['_id'] = val
                    else:
                        rowSet[header[i]] = val
            if id_col == None:
                rowSet['_id'] = str(rowCount)
            table.append(rowSet)
    return table;

def validate_data(rows,schema,convert_types=False,remove_nones=False,remove_invalids=False,map_categories=False,assign_ids=False,remove_extra_fields=False):
    return _validate(rows,schema,convert_types=convert_types,ignore_nones=False,remove_nones=remove_nones,remove_invalids=remove_invalids,map_categories=map_categories,ignore_ids=False,assign_ids=assign_ids,remove_extra_fields=remove_extra_fields)

def validate_predictions(predictions,schema,convert_types=False,remove_invalids=False,remove_extra_fields=False):
    return _validate(predictions,schema,convert_types=convert_types,ignore_nones=True,remove_nones=False,remove_invalids=remove_invalids,map_categories=False,ignore_ids=True,assign_ids=False,remove_extra_fields=remove_extra_fields)

def _validate(rows,schema,convert_types,ignore_nones,remove_nones,remove_invalids,map_categories,ignore_ids,assign_ids,remove_extra_fields):
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
        if assign_ids:
            r['_id'] = str(i)
        elif not(ignore_ids):
            if not(r.has_key('_id')):
                raise DataValidationException("Row:'"+str(i)+"' is missing Field:'_id'",row=i,field='_id')
            if convert_types:
                r['_id'] = str(r['_id'])
            if not (type(r['_id']) == str):
                raise DataValidationException("Row:'"+str(i)+"' Field:'_id' Value:'"+str(r['_id'])+"' is "+str(type(r['_id']))+", not a str",row=i,field='_id')
            if prevIDs.has_key(r['_id']):
                raise DataValidationException("Row:'"+str(i)+"' Field:'_id' Value:'"+str(r['_id'])+"' is not unique, conflicts with Row:'"+str(prevIDs[r['_id']])+"'",row=i,field='_id')
            prevIDs[r['_id']] = i
        elif r.has_key('_id'):
            if remove_extra_fields:
                r.pop('_id')
            else:
                raise DataValidationException("Row:'"+str(i)+"' Field:'_id' should not be included",row=i,field='_id')                    
                
        for c in r.keys():
            if not(c == '_id'):
                if not(schema.has_key(c)):
                    if remove_extra_fields:
                        r.pop(c)
                    else:
                        raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' is not defined in schema",row=i,field=c)                    
                elif r[c] == None:
                    if remove_nones:
                        r.pop(c)
                    else:
                        if not(ignore_nones):
                            raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' should be removed because it has value None",row=i,field=c)                    
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
                                raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not an int",row=i,field=c)                            
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
                                raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a float",row=i,field=c)
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
                                raise DataValidationException("Row:'"+str(i)+"' Field:'"+c+"' Value:'"+str(r[c])+"' is "+str(type(r[c]))+", not a bool",row=i,field=c)   
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
                raise DataValidationException("Categorical field '"+c+"' has "+str(len(catCounts[c].keys()))+" unique values which exceeds the limit of "+str(maxCats),field=c)
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
        if (fill == 0 and not(ignore_nones)):
            raise DataValidationException("Field '"+c+"' does not have any values",field=c)


def point_estimate(predictions, schema, column):
    col_type = schema[column]['type']
    if col_type == 'boolean' or col_type == 'categorical':
        # mode
        counts = _counts(predictions, column)
        max_count = 0
        max_value = None
        for value in counts:
            if counts[value] > max_count:
                max_count = counts[value]
                max_value = value
        return max_value
    elif col_type == 'count':
        # median
        values = _sorted_values(predictions, column)
        N = len(values)
        if N % 2 == 0: # even
            a = N / 2 - 1
            return values[a] + values[a + 1] / 2.
        else: # odd
            a = int((N - 1) / 2)
            return values[a]
    elif col_type == 'real':
        # mean
        values = [row[column] for row in predictions]
        mean = sum(values) / len(values)
        return mean
    else:
        assert False, 'bad column type'


def credible_values(predictions, schema, column, p=None):
    col_type = schema[column]['type']
    if col_type == 'boolean' or col_type == 'categorical':
        if p == None:
            p = .5
        freqs = _freqs(_counts(predictions, column))
        sorted_freqs = sorted(freqs.items(), key=lambda x: x[1], reverse=True)
        threshold_freqs = [(c, a) for c, a in sorted_freqs if a >= p]
        return threshold_freqs
    elif col_type == 'count' or col_type == 'real':
        # Note: this computes an interval that removes equal probability mass 
        # from each end; a possible alternative would be to return the shorted 
        # interval containing the given amount of mass
        if p == None:
            p = .9
        N = len(predictions)
        a = int(round(N * (1. - p) / 2.))
        sorted_values = _sorted_values(predictions, column)
        N = len(sorted_values)
        lo = sorted_values[a]
        hi = sorted_values[N - 1 - a]
        return (lo, hi)
    else:
        assert False, 'bad column type'


def prob_within(predictions, schema, column, set_spec):
    col_type = schema[column]['type']
    if col_type == 'boolean' or col_type == 'categorical':
        count = 0
        for row in predictions:
            if row[column] in set_spec:
                count += 1
        return float(count) / len(predictions)
    elif col_type == 'count' or col_type == 'real':
        count = 0
        mn = set_spec[0]
        mx = set_spec[1]
        for row in predictions:
            v = row[column]
            if (mn == None or v >= mn) and (mx == None or v <= mx):
                count += 1
        return float(count) / len(predictions)                
    else:
        assert False, 'bad column type'


def binned_values(predictions, schema, column, num_bins=None):
    col_type = schema[column]['type']
    col_type = schema[column]['type']
    if col_type == 'boolean' or col_type == 'categorical':
        freqs = _freqs(_counts(predictions, column))
        return freqs.items()
    elif col_type == 'count' or col_type == 'real':
        #FIXME count bins need to be integer-bounded
        N = len(predictions)
        if num_bins == None:
            num_bins = int(ceil(log(N) * 2.))
        sorted_values = _sorted_values(predictions, column)
        mn = sorted_values[0]
        mx = sorted_values[-1]
        bw = float(mx - mn) / num_bins
        bins = [(i * bw, (i + 1) * bw) for i in range(num_bins)]
        bin_counts = [0] * num_bins
        for row in predictions:
            v = row[column]
            bi = int(floor((v - mn) / bw))
            bi = min(num_bins - 1, bi)
            bin_counts[bi] += 1
        return zip(bins, [float(c) / N for c in bin_counts])
    else:
        assert False, 'bad column type'


def _sorted_values(predictions, column):
    values = [row[column] for row in predictions]
    values.sort()
    return values


def _counts(predictions, column):
    counts = {}
    for row in predictions:
        counts[row[column]] = counts.get(row[column], 0) + 1
    return counts


def _freqs(counts):
    total = sum(counts.values())
    freqs = dict([(k, float(counts[k]) / total) for k in counts])
    return freqs


def summarize(predictions, col):
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
