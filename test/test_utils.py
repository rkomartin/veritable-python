#! usr/bin/python
# coding=utf-8

from veritable.utils import (write_csv, read_csv, make_schema,
    validate_data, validate_predictions, _format_url, clean_data,
    clean_predictions, _validate_schema)
from veritable.exceptions import VeritableError
from nose.tools import raises, assert_raises
from tempfile import mkstemp
import csv
import json
import os

INVALID_IDS = ["\xc3\xa9l\xc3\xa9phant", "374.34", "ajfh/d/sfd@#$",
    "\xe3\x81\xb2\xe3\x81\x9f\xe3\x81\xa1\xe3\x81\xae", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar", 5, 374.34, False,
    "_underscores"]


def test_format_url():
    assert 'base/path' == _format_url(['base', 'path'])
    assert '/base/path' == _format_url(['/base', 'path'],
        noquote=[0])
    assert 'base/path/path2' == _format_url(['base/path', 'path2'],
        noquote=[0])
    assert '/base/path/path2' == _format_url(['/base/path', 'path2'],
        noquote=[0])


def test_write_read_csv():
    handle, filename = mkstemp()
    refrows = [{'_id': '7', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a'},
               {'_id': '8', 'ColInt':4, 'ColCat': 'b', 'ColBool':False},
               {'_id': '9'}]
    write_csv(refrows, filename, dialect=csv.excel)
    testrows = read_csv(filename, dialect=csv.excel)
    cschema = {
        'ColInt': {'type': 'count'},
        'ColFloat': {'type': 'real'},
        'ColCat': {'type': 'categorical'},
        'ColBool': {'type': 'boolean'}
        }
    clean_data(testrows, cschema)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        assert testrows[i] == refrows[i]
    os.remove(filename)


def test_read_csv_map_id():
    handle, filename = mkstemp()
    refrows = [
        {'myID': '7', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
         'ColBool':True},
        {'myID': '8', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False},
        {'myID': '9'}]
    write_csv(refrows, filename, dialect=csv.excel)
    testrows = read_csv(filename, id_col='myID', dialect=csv.excel)
    cschema = {
        'ColInt': {'type': 'count'},
        'ColFloat': {'type': 'real'},
        'ColCat': {'type': 'categorical'},
        'ColBool': {'type': 'boolean'}
        }
    clean_data(testrows, cschema)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        refrows[i]['_id'] = refrows[i]['myID']
        refrows[i].pop('myID')
        assert testrows[i] == refrows[i]
    os.remove(filename)


def test_read_csv_assign_id():
    handle, filename = mkstemp()
    refrows = [{'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
                {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False},
                {}]
    write_csv(refrows, filename, dialect=csv.excel)
    testrows = read_csv(filename, dialect=csv.excel)
    cschema = {
        'ColInt': {'type': 'count'},
        'ColFloat': {'type': 'real'},
        'ColCat': {'type': 'categorical'},
        'ColBool': {'type': 'boolean'}
        }
    clean_data(testrows, cschema)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        refrows[i]['_id'] = str(i + 1)
        assert testrows[i] == refrows[i]
    os.remove(filename)


def test_make_schema_headers():
    refSchema = {'CatA': {'type': 'categorical'},
                 'CatB': {'type': 'categorical'},
                 'IntA': {'type': 'count'},
                 'IntB': {'type': 'count'}}
    headers = ['IntA', 'IntB', 'CatA', 'CatB', 'Foo']
    schemaRule1 = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    schemaRule2 = [[lambda h, v: h[0:3] == 'Cat',
                    {'type': 'categorical'}],
                   [lambda h, v: h[0:3] == 'Int',
                    {'type': 'count'}]]
    schema = make_schema(schemaRule1, headers=headers)
    assert schema == refSchema
    schema = make_schema(schemaRule2, headers=headers)
    assert schema == refSchema


def test_make_schema_rows():
    refSchema = {'CatA': {'type': 'categorical'},
                    'CatB': {'type': 'categorical'},
                    'IntA': {'type': 'count'},
                    'IntB': {'type': 'count'}}
    rows = [{'CatA':None, 'CatB':None, 'IntA':None, 'IntB':None, 'Foo':None}]
    schemaRule = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    schemaRule2 = [[lambda h, v: h[0:3] == 'Cat',
                    {'type': 'categorical'}],
                   [lambda h, v: h[0:3] == 'Int',
                    {'type': 'count'}]]
    schema = make_schema(schemaRule, rows=rows)
    assert schema == refSchema
    schema = make_schema(schemaRule2, rows=rows)
    assert schema == refSchema


@raises(Exception)
def test_make_schema_noarg1_fail():
    schemaRule = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    make_schema(schemaRule)


@raises(Exception)
def test_make_schema_noarg2_fail():
    schemaRule = [[lambda x: x[0:4] == 'Cat',
                    {'type': 'categorical'}],
                   [lambda x: x[0:4] == 'Int',
                    {'type': 'count'}]]
    make_schema(schemaRule)

# Invalid Schema
def test_missing_schema_type_fail():
    bschema = {'ColInt': {}, 'ColFloat': {'type': 'real'}}
    testrows = []
    assert_raises(VeritableError, validate_data, testrows, bschema)
    assert_raises(VeritableError, clean_data, testrows, bschema)


def test_bad_schema_type_fail():
    bschema = {'ColInt': {'type': 'jello'}, 'ColFloat': {'type': 'real'}}
    testrows = []
    assert_raises(VeritableError, validate_data, testrows, bschema)
    assert_raises(VeritableError, clean_data, testrows, bschema)

def test_unicode_schema_py2():
    _validate_schema(json.loads(json.dumps({'a': {'type': 'real'}})))

def test_invalid_schema_underscore():
    assert_raises(VeritableError, _validate_schema, {'_foo': {'type': 'count'}})

def test_invalid_schema_dot():
    assert_raises(VeritableError, _validate_schema, {'b.d': {'type': 'count'}})

def test_invalid_schema_dollar():
    assert_raises(VeritableError, _validate_schema, {'b$d': {'type': 'count'}})


vschema = {
    'ColInt': {'type': 'count'},
    'ColFloat': {'type': 'real'},
    'ColCat': {'type': 'categorical'},
    'ColBool': {'type': 'boolean'}
    }


# Valid
def test_data_valid_rows():
    refrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False},
        {'_id': '3'}]
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False},
        {'_id': '3'}]
    validate_data(testrows, vschema)
    assert testrows == refrows
    clean_data(testrows, vschema)
    assert testrows == refrows



def test_pred_valid_rows_no_id():
    refrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    assert testrows == refrows
    clean_predictions(testrows, vschema)
    assert testrows != refrows


def test_pred_valid_rows_id():
    refrows = [
        {'_request_id': '0', 'ColInt':None, 'ColFloat':None,
         'ColCat': 'a', 'ColBool':True},
        {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1,
         'ColCat':None, 'ColBool':False},
        {'_request_id': '2', 'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'_request_id': '0', 'ColInt':None, 'ColFloat':None,
         'ColCat': 'a', 'ColBool':True},
        {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1,
         'ColCat':None, 'ColBool':False},
        {'_request_id': '2', 'ColInt':None, 'ColFloat':None}]
    validate_predictions(testrows, vschema)
    assert testrows == refrows
    clean_predictions(testrows, vschema)
    assert testrows == refrows


def test_data_valid_rows_fix():
    refrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False},
        {'_id': '3'}]
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False},
        {'_id': '3'}]
    clean_data(testrows, vschema)
    assert testrows == refrows


def test_pred_valid_rows_fix():
    refrows = [
        {'_request_id': '0', 'ColInt':None, 'ColFloat':None,
         'ColCat': 'a', 'ColBool':True},
        {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False},
        {'_request_id': '2', 'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    clean_predictions(testrows, vschema)
    assert testrows == refrows


# Missing ID
def test_data_missing_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1


def test_data_missing_id_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    clean_data(testrows, vschema, assign_ids=True)
    assert testrows[0]['_id'] != testrows[1]['_id']
    validate_data(testrows, vschema)


# Duplicate ID
def test_data_duplicate_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '1', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == '_id'


# Non-string ID
@raises(VeritableError)
def test_data_nonstring_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == '_id'
        raise


def test_data_nonstring_id_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    clean_data(testrows, vschema)
    assert testrows[1]['_id'] == '2'
    validate_data(testrows, vschema)


def test_data_nonalphanumeric_ids_fail():
    for id in INVALID_IDS:
        testrows = [
            {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
             'ColBool':True},
            {'_id': id, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
             'ColBool':False}]
        assert_raises(VeritableError, validate_data, testrows,
            vschema)


# Extra Field Not In Schema
def test_data_extrafield_pass():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    validate_data(testrows, vschema)
    assert testrows[1]['ColEx'] == 4


def test_pred_extrafield_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColEx':None, 'ColInt':4, 'ColFloat':None,
         'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColEx'


def test_pred_idfield_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == '_request_id'


def test_data_extrafield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema, remove_extra_fields=True)
    assert not('ColEx' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_extrafield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColEx':None, 'ColInt':4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert not('_id' in testrows[0])
    assert not('ColEx' in testrows[1])
    validate_predictions(testrows, vschema)


# Field value is None
def test_data_nonefield_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_data_nonefield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert not('ColCat' in testrows[1])
    validate_data(testrows, vschema)


# Non-int Count
def test_data_non_int_count_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': '4', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows,
        vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_data_count_limit_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 100001, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows,
        vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_non_int_count_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt': '4', 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_data_non_int_count_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': '4', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert testrows[1]['ColInt'] == 4
    validate_data(testrows, vschema)


def test_pred_non_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': '4', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert testrows[1]['ColInt'] == 4
    validate_predictions(testrows, vschema)


# Non-valid-int Count
def test_data_nonvalid_int_count_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_nonvalid_int_count_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_int_count_limit_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt': 100001, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_int_count_limit_fix():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt': 100001, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert 'ColInt' not in testrows[1]
    validate_predictions(testrows, vschema)


def test_data_nonvalid_int_count_fixfail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, clean_data, testrows, vschema,
        remove_invalids=False)
    try:
        clean_data(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_nonvalid_int_count_fixfail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, clean_predictions, testrows,
        vschema, remove_invalids=False)
    try:
        clean_predictions(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_data_nonvalid_int_count_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert not('ColInt' in testrows[1])
    validate_data(testrows, vschema)


def test_data_int_count_limit_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 100001, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert not('ColInt' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert not('ColInt' in testrows[1])
    validate_predictions(testrows, vschema)

def test_data_negative_int_count_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': -4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows,
        vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_negative_int_count_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt': -4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'

def test_data_negative_int_count_fixfail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': -3, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, clean_data, testrows, vschema,
        remove_invalids=False)
    try:
        clean_data(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_negative_int_count_fixfail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': -3, 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, clean_predictions, testrows,
        vschema, remove_invalids=False)
    try:
        clean_predictions(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColInt'

def test_data_negative_int_count_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': -3, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, clean_data, testrows, vschema,
        remove_invalids=False)
    clean_data(testrows, vschema)
    assert not('ColInt' in testrows[1])


def test_pred_negative_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': -3, 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, clean_predictions, testrows,
        vschema, remove_invalids=False)
    clean_predictions(testrows, vschema)
    assert not('ColInt' in testrows[1])


# Non-float Real
def test_data_non_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': '4.1', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_pred_non_float_real_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt':None, 'ColFloat': '4.1', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_non_float_real_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': '4.1', 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert testrows[1]['ColFloat'] == 4.1
    validate_data(testrows, vschema)


def test_pred_non_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': '4.1', 'ColCat': 'b', 'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert testrows[1]['ColFloat'] == 4.1
    validate_predictions(testrows, vschema)


# Non-valid-float Real
def test_data_nonvalid_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'

def test_data_nan_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': float('NaN'), 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'

def test_data_inf_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': float('Inf'), 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'



def test_pred_nonvalid_float_real_fail():
    testrows = [
        {'_request_id': '0', 'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a',
         'ColBool':True},
        {'_request_id': '1', 'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_nonvalid_float_real_fixfail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(VeritableError, clean_data, testrows, vschema,
        remove_invalids=False)
    try:
        clean_data(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_pred_nonvalid_float_real_fixfail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b', 'ColBool':False}]
    assert_raises(VeritableError, clean_predictions, testrows,
        vschema, remove_invalids=False)
    try:
        clean_predictions(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_nonvalid_float_real_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    clean_data(testrows, vschema)
    assert not('ColFloat' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b', 'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert not('ColFloat' in testrows[1])
    validate_predictions(testrows, vschema)


# Non-str Category
def test_data_non_str_cat_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_pred_non_str_cat_fail():
    testrows = [{'_request_id': '0', 'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a',
                 'ColBool':True},
                {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1, 'ColCat':3,
                 'ColBool':False}]
    assert_raises(VeritableError, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_data_non_str_cat_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    clean_data(testrows, vschema)
    assert testrows[1]['ColCat'] == '3'
    validate_data(testrows, vschema)


def test_pred_non_str_cat_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    clean_predictions(testrows, vschema)
    assert testrows[1]['ColCat'] == '3'
    validate_predictions(testrows, vschema)


# Non-bool boolean
def test_data_non_bool_boolean_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '0'}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_non_bool_boolean_fail():
    testrows = [{'_request_id': '0', 'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a',
                 'ColBool':True},
                {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b',
                 'ColBool': '0'}]
    assert_raises(VeritableError, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_data_non_bool_boolean_truefix():
    testrows = [
    {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
    {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '1'},
    {'_id': '4', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '2'},
    {'_id': '5', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'True'},
    {'_id': '6', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'true'},
    {'_id': '7', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'Yes'},
    {'_id': '8', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'YES'},
    {'_id': '9', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'Y'},
    {'_id': '10', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'y'}]
    clean_data(testrows, vschema)
    for r in testrows:
        assert r['ColBool'] == True
    validate_data(testrows, vschema)


def test_pred_non_bool_boolean_truefix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '1'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '2'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'True'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'true'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'Yes'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'YES'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'Y'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'y'}]
    clean_predictions(testrows, vschema)
    for r in testrows:
        assert r['ColBool'] == True
    validate_predictions(testrows, vschema)


def test_data_non_bool_boolean_falsefix():
    testrows = [
    {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':False},
    {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '0'},
    {'_id': '5', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'False'},
    {'_id': '6', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'false'},
    {'_id': '7', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'No'},
    {'_id': '8', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'NO'},
    {'_id': '9', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'N'},
    {'_id': '10', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'n'}]
    clean_data(testrows, vschema)
    for r in testrows:
        assert r['ColBool'] == False
    validate_data(testrows, vschema)


def test_pred_non_bool_boolean_falsefix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':False},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '0'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'False'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'false'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'No'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'NO'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'N'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'n'}]
    clean_predictions(testrows, vschema)
    for r in testrows:
        assert r['ColBool'] == False
    validate_predictions(testrows, vschema)


# Non-valid-bool boolean
def test_data_nonvalid_bool_boolean_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool': 'jello'}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_nonvalid_bool_boolean_fail():
    testrows = [{'_request_id': '0', 'ColInt':None, 'ColFloat':3.1,
                 'ColCat': 'a', 'ColBool':True},
                {'_request_id': '1', 'ColInt':None, 'ColFloat':4.1,
                 'ColCat': 'b', 'ColBool': 'jello'}]
    assert_raises(VeritableError, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_data_nonvalid_bool_boolean_fixfail():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
        'ColBool':True}, {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
        'ColBool': 'jello'}]
    assert_raises(VeritableError, validate_data, testrows, vschema)
    try:
        clean_data(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_nonvalid_bool_boolean_fixfail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'jello'}]
    assert_raises(VeritableError, clean_predictions, testrows,
        vschema, remove_invalids=False)
    try:
        clean_predictions(testrows, vschema, remove_invalids=False)
    except VeritableError as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_data_nonvalid_bool_boolean_fix():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
        'ColBool':True}, {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
        'ColBool': 'jello'}]
    clean_data(testrows, vschema)
    assert not('ColBool' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_bool_boolean_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'jello'}]
    clean_predictions(testrows, vschema)
    assert not('ColBool' in testrows[1])
    validate_predictions(testrows, vschema)


# Too many categories
def test_data_too_many_cats_fail():
    eschema = {
        'ColCat': {'type': 'categorical'}
    }
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols - 1):
        testrows.append({'_id':str(rid), 'ColCat':str(i)})
        testrows.append({'_id':str(rid + 1), 'ColCat':str(i)})
        rid = rid + 2
    testrows.append({'_id':str(rid), 'ColCat':str(maxCols - 1)})
    testrows.append({'_id':str(rid + 1), 'ColCat':str(maxCols)})
    assert_raises(VeritableError, validate_data,
        testrows, eschema)
    try:
        validate_data(testrows, eschema)
    except VeritableError as e:
        assert e.col == 'ColCat'


def test_pred_too_many_cats_fail():
    eschema = {
        'ColCat': {'type': 'categorical'}
    }
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols - 1):
        testrows.append({'_request_id': str(rid), 'ColCat':str(i)})
        rid += 1
        testrows.append({'_request_id': str(rid), 'ColCat':str(i)})
        rid += 1
    testrows.append({'_request_id': str(rid),'ColCat':str(maxCols - 1)})
    testrows.append({'_request_id': str(rid+1),'ColCat':str(maxCols)})
    assert_raises(VeritableError,
        validate_predictions, testrows, eschema)
    try:
        validate_predictions(testrows, eschema)
    except VeritableError as e:
        assert e.col == 'ColCat'


def test_data_too_many_cats_fix():
    eschema = {
        'ColCat': {'type': 'categorical'}
    }
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols - 1):
        testrows.append({'_id':str(rid), 'ColCat':str(i)})
        testrows.append({'_id':str(rid + 1), 'ColCat':str(i)})
        rid = rid + 2
    testrows.append({'_id':str(rid), 'ColCat':str(maxCols - 1)})
    testrows.append({'_id':str(rid + 1), 'ColCat':str(maxCols)})
    clean_data(testrows, eschema)
    assert testrows[510]['ColCat'] == 'Other'
    assert testrows[511]['ColCat'] == 'Other'
    validate_data(testrows, eschema)


def test_data_empty_col_fail():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColBool':True},
                {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColBool':False}]
    assert_raises(VeritableError, validate_data,
        testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except VeritableError as e:
        assert e.col == 'ColCat'


