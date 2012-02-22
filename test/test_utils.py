from veritable.utils import read_csv, write_csv, make_schema, validate
from veritable.exceptions import DataValidationException
from nose.tools import raises
from tempfile import mkstemp
import csv
import os

def test_write_read_csv():
    handle,filename = mkstemp()
    refrows = [{'_id':'7', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'_id':'9', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    write_csv(refrows,filename,dialect=csv.excel)
    testrows = read_csv(filename,dialect=csv.excel)
    cschema = {
        'ColInt':{'type':'count'},
        'ColFloat':{'type':'real'},
        'ColCat':{'type':'categorical'},
        'ColBool':{'type':'boolean'}
        }
    validate(testrows,cschema,convertTypes=True)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        assert testrows[i] == refrows[i]
    os.remove(filename)

def test_read_csv_map_id():
    handle,filename = mkstemp()
    refrows = [{'myID':'7', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'myID':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'myID':'9', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    write_csv(refrows,filename,dialect=csv.excel)
    testrows = read_csv(filename,idCol='myID',dialect=csv.excel)
    cschema = {
        'ColInt':{'type':'count'},
        'ColFloat':{'type':'real'},
        'ColCat':{'type':'categorical'},
        'ColBool':{'type':'boolean'}
        }
    validate(testrows,cschema,convertTypes=True)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        refrows[i]['_id'] = refrows[i]['myID']
        refrows[i].pop('myID')
        assert testrows[i] == refrows[i]
    os.remove(filename)

def test_read_csv_assign_id():
    handle,filename = mkstemp()
    refrows = [{'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    write_csv(refrows,filename,dialect=csv.excel)
    testrows = read_csv(filename,dialect=csv.excel)
    cschema = {
        'ColInt':{'type':'count'},
        'ColFloat':{'type':'real'},
        'ColCat':{'type':'categorical'},
        'ColBool':{'type':'boolean'}
        }
    validate(testrows,cschema,convertTypes=True)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        refrows[i]['_id'] = str(i+1)
        assert testrows[i] == refrows[i]
    os.remove(filename)


def test_make_schema_headers():
    refSchema = {'CatA': {'type': 'categorical'},
                    'CatB': {'type': 'categorical'},
                    'IntA': {'type': 'count'},
                    'IntB': {'type': 'count'}}
    headers = ['IntA','IntB','CatA','CatB','Foo']
    schemaRule = [  ['Int.*',{'type':'count'}],
                    ['Cat.*',{'type':'categorical'}]  ]
    schema = make_schema(schemaRule,headers=headers)
    assert schema == refSchema

def test_make_schema_rows():
    refSchema = {'CatA': {'type': 'categorical'},
                    'CatB': {'type': 'categorical'},
                    'IntA': {'type': 'count'},
                    'IntB': {'type': 'count'}}
    rows = [{'CatA':None, 'CatB':None, 'IntA':None, 'IntB':None, 'Foo':None}]
    schemaRule = [  ['Int.*',{'type':'count'}],
                    ['Cat.*',{'type':'categorical'}]  ]
    schema = make_schema(schemaRule,rows=rows)
    assert schema == refSchema

@raises(Exception)
def test_make_schema_noarg_fail():
    schemaRule = [  ['Int.*',{'type':'count'}],
                    ['Cat.*',{'type':'categorical'}]  ]
    schema = make_schema(schemaRule)


# Invalid Schema
@raises(DataValidationException)
def test_missing_schema_type_fail():
    bschema = {'ColInt':{},'ColFloat':{'type':'real'} }
    testrows = []
    try:
        validate(testrows, bschema)
    except DataValidationException as e:
        assert e.field == 'ColInt'
        raise

@raises(DataValidationException)
def test_bad_schema_type_fail():
    bschema = {'ColInt':{'type':'jello'},'ColFloat':{'type':'real'} }
    testrows = []
    try:
        validate(testrows, bschema)
    except DataValidationException as e:
        assert e.field == 'ColInt'
        raise


vschema = {
    'ColInt':{'type':'count'},
    'ColFloat':{'type':'real'},
    'ColCat':{'type':'categorical'},
    'ColBool':{'type':'boolean'}
    }

# Valid
def test_valid_rows():
    refrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'_id':'3', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'_id':'3', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    validate(testrows, vschema)
    assert testrows == refrows

def test_valid_rows_fix():
    refrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'_id':'3', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {'_id':'3', 'ColInt':None, 'ColFloat':None, 'ColCat':None, 'ColBool':None}]
    validate(testrows, vschema, convertTypes=True, nullInvalids=True, mapCategories=True, removeExtraFields=True)
    assert testrows == refrows

# Missing ID
@raises(DataValidationException)
def test_missing_id_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        raise
        
def test_missing_id_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, assignIDs=True)
    assert testrows[0]['_id'] != testrows[1]['_id']
    validate(testrows, vschema)
    

# Duplicate ID
@raises(DataValidationException)
def test_duplicate_id_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'1', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == '_id'
        raise
    
# Non-string ID
@raises(DataValidationException)
def test_nonstring_id_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == '_id'
        raise

def test_nonstring_id_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True)
    assert testrows[1]['_id'] == '2'
    validate(testrows, vschema)

# Extra Field Not In Schema
@raises(DataValidationException)
def test_extrafield_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColEx'
        raise

def test_extrafield_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, removeExtraFields=True)
    assert not(testrows[1].has_key('ColEx'))
    validate(testrows, vschema)

# Non-int Count
@raises(DataValidationException)
def test_non_int_count_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':'4', 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColInt'
        raise

def test_non_int_count_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':'4', 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True)
    assert testrows[1]['ColInt'] == 4
    validate(testrows, vschema)

# Non-valid-int Count
@raises(DataValidationException)
def test_nonvalid_int_count_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColInt'
        raise

@raises(DataValidationException)
def test_nonvalid_int_count_fixfail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema, convertTypes=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColInt'
        raise

def test_nonvalid_int_count_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True, nullInvalids=True)
    assert testrows[1]['ColInt'] == None
    validate(testrows, vschema)


# Non-float Real
@raises(DataValidationException)
def test_non_float_real_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':'4.1', 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColFloat'
        raise

def test_non_float_real_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':'4.1', 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True)
    assert testrows[1]['ColFloat'] == 4.1
    validate(testrows, vschema)

# Non-valid-float Real
@raises(DataValidationException)
def test_nonvalid_float_real_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColFloat'
        raise

@raises(DataValidationException)
def test_nonvalid_float_real_fixfail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    try:
        validate(testrows, vschema, convertTypes=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColFloat'
        raise

def test_nonvalid_float_real_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True, nullInvalids=True)
    assert testrows[1]['ColFloat'] == None
    validate(testrows, vschema)


# Non-str Category
@raises(DataValidationException)
def test_non_str_cat_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColCat'
        raise

def test_non_str_cat_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    validate(testrows, vschema, convertTypes=True)
    assert testrows[1]['ColCat'] == '3'
    validate(testrows, vschema)

# Non-bool boolean
@raises(DataValidationException)
def test_non_bool_boolean_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColBool'
        raise

def test_non_bool_boolean_truefix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'1'},
                {'_id':'4', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'2'},
                {'_id':'5', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'True'},
                {'_id':'6', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'true'},
                {'_id':'7', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Yes'},
                {'_id':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'YES'},
                {'_id':'9', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Y'},
                {'_id':'10', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'y'}]
    validate(testrows, vschema, convertTypes=True)
    for r in testrows:
        assert r['ColBool'] == True
    validate(testrows, vschema)

def test_non_bool_boolean_falsefix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':False},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'},
                {'_id':'5', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'False'},
                {'_id':'6', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'false'},
                {'_id':'7', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'No'},
                {'_id':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'NO'},
                {'_id':'9', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'N'},
                {'_id':'10', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'n'}]
    validate(testrows, vschema, convertTypes=True)
    for r in testrows:
        assert r['ColBool'] == False
    validate(testrows, vschema)

# Non-valid-bool boolean
@raises(DataValidationException)
def test_nonvalid_bool_boolean_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColBool'
        raise

@raises(DataValidationException)
def test_nonvalid_bool_boolean_fixfail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    try:
        validate(testrows, vschema, convertTypes=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.field == 'ColBool'
        raise

def test_nonvalid_bool_boolean_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    validate(testrows, vschema, convertTypes=True, nullInvalids=True)
    assert testrows[1]['ColBool'] == None
    validate(testrows, vschema)

# Too many categories
@raises(DataValidationException)
def test_too_many_cats_fail():
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols-1):
        testrows.append({'_id':str(rid), 'ColCat':str(i)})
        testrows.append({'_id':str(rid+1), 'ColCat':str(i)})
        rid = rid + 2
    testrows.append({'_id':str(rid), 'ColCat':str(maxCols-1)})
    testrows.append({'_id':str(rid+1), 'ColCat':str(maxCols)})
    try:
        validate(testrows, vschema, convertTypes=True)
    except DataValidationException as e:
        assert e.field == 'ColCat'
        raise

def test_too_many_cats_fix():
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols-1):
        testrows.append({'_id':str(rid), 'ColCat':str(i)})
        testrows.append({'_id':str(rid+1), 'ColCat':str(i)})
        rid = rid + 2
    testrows.append({'_id':str(rid), 'ColCat':str(maxCols-1)})
    testrows.append({'_id':str(rid+1), 'ColCat':str(maxCols)})
    validate(testrows, vschema, mapCategories=True)
    assert testrows[510]['ColCat'] == 'Other'
    assert testrows[511]['ColCat'] == 'Other'
    validate(testrows, vschema)


@raises(DataValidationException)
def test_empty_col_fail():
    testrows = [{'_id':'1', 'ColCat':None},
                {'_id':'2', 'ColCat':None}]
    try:
        validate(testrows, vschema)
    except DataValidationException as e:
        assert e.field == 'ColCat'
        raise




