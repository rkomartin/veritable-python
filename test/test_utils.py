#! usr/bin/python
# coding=utf-8

from copy import deepcopy
from veritable.utils import (write_csv, read_csv, make_schema, summarize,
    validate_data, validate_predictions, _format_url, clean_data,
    clean_predictions)
from veritable.exceptions import DataValidationException
from nose.tools import raises, assert_raises
from tempfile import mkstemp
import csv
import os

INVALID_IDS = ["\xc3\xa9l\xc3\xa9phant", "374.34", "ajfh/d/sfd@#$",
    "\xe3\x81\xb2\xe3\x81\x9f\xe3\x81\xa1\xe3\x81\xae", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar", 5, 374.34, False]


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
    validate_data(testrows, cschema, convert_types=True)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        assert testrows[i] == refrows[i]
    testrows = read_csv(filename, dialect=csv.excel)
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
    validate_data(testrows, cschema, convert_types=True)
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
    validate_data(testrows, cschema, convert_types=True)
    assert len(testrows) == len(refrows)
    for i in range(len(testrows)):
        refrows[i]['_id'] = str(i + 1)
        assert testrows[i] == refrows[i]
    testrows = read_csv(filename, dialect=csv.excel)
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
    schemaRule = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    schema = make_schema(schemaRule, headers=headers)
    assert schema == refSchema


def test_make_schema_rows():
    refSchema = {'CatA': {'type': 'categorical'},
                    'CatB': {'type': 'categorical'},
                    'IntA': {'type': 'count'},
                    'IntB': {'type': 'count'}}
    rows = [{'CatA':None, 'CatB':None, 'IntA':None, 'IntB':None, 'Foo':None}]
    schemaRule = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    schema = make_schema(schemaRule, rows=rows)
    assert schema == refSchema


@raises(Exception)
def test_make_schema_noarg_fail():
    schemaRule = [['Int.*', {'type': 'count'}],
                  ['Cat.*', {'type': 'categorical'}]]
    make_schema(schemaRule)


# Invalid Schema
def test_missing_schema_type_fail():
    bschema = {'ColInt': {}, 'ColFloat': {'type': 'real'}}
    testrows = []
    assert_raises(DataValidationException, validate_data, testrows, bschema)
    assert_raises(DataValidationException, clean_data, testrows, bschema)


def test_bad_schema_type_fail():
    bschema = {'ColInt': {'type': 'jello'}, 'ColFloat': {'type': 'real'}}
    testrows = []
    assert_raises(DataValidationException, validate_data, testrows, bschema)
    assert_raises(DataValidationException, clean_data, testrows, bschema)


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

def test_pred_valid_rows():
    refrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
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
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True, remove_nones=True,
        remove_invalids=True, reduce_categories=True,
        remove_extra_fields=True)
    assert testrows == refrows
    clean_data(tr, vschema)
    assert testrows == refrows


def test_pred_valid_rows_fix():
    refrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True, remove_extra_fields=True)
    assert testrows == refrows
    clean_predictions(tr, vschema)
    assert tr == refrows


# Missing ID
def test_data_missing_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1


def test_data_missing_id_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, assign_ids=True)
    assert testrows[0]['_id'] != testrows[1]['_id']
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert tr[0]['_id'] != tr[1]['_id']
    validate_data(tr, vschema)


# Duplicate ID
def test_data_duplicate_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '1', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'


# Non-string ID
@raises(DataValidationException)
def test_data_nonstring_id_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'
        raise


def test_data_nonstring_id_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['_id'] == '2'
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert tr[1]['_id'] == '2'
    validate_data(tr, vschema)


def test_data_nonalphanumeric_ids_fail():
    for id in INVALID_IDS:
        testrows = [
            {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
             'ColBool':True},
            {'_id': id, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
             'ColBool':False}]
        assert_raises(DataValidationException, validate_data, testrows,
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
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColEx':None, 'ColInt':4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColEx'


def test_pred_idfield_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'


def test_data_extrafield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, remove_extra_fields=True)
    assert not('ColEx' in testrows[1])
    validate_data(testrows, vschema)
    clean_data(tr, vschema, remove_extra_fields=True)
    assert not('ColEx' in tr[1])
    validate_data(tr, vschema)


def test_pred_extrafield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColEx':None, 'ColInt':4, 'ColFloat':None, 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, remove_extra_fields=True)
    assert not('_id' in testrows[0])
    assert not('ColEx' in testrows[1])
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert not('_id' in tr[0])
    assert not('ColEx' in tr[1])
    validate_predictions(tr, vschema)


# Field value is None
def test_data_nonefield_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_data_nonefield_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, remove_nones=True)
    assert not('ColCat' in testrows[1])
    validate_data(testrows, vschema)
    clean_data(tr, vschema, remove_nones=True)
    assert not('ColCat' in tr[1])
    validate_data(tr, vschema)


# Non-int Count
def test_data_non_int_count_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': '4', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows,
        vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_non_int_count_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': '4', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_data_non_int_count_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': '4', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColInt'] == 4
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert testrows[1]['ColInt'] == 4
    validate_data(testrows, vschema)


def test_pred_non_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': '4', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColInt'] == 4
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert tr[1]['ColInt'] == 4
    validate_predictions(tr, vschema)


# Non-valid-int Count
def test_data_nonvalid_int_count_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_nonvalid_int_count_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


@raises(DataValidationException)
def test_data_nonvalid_int_count_fixfail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema,
        convert_types=True)
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_pred_nonvalid_int_count_fixfail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema, convert_types=True)
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'


def test_data_nonvalid_int_count_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt': 'jello', 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColInt' in testrows[1])
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert not('ColInt' in tr[1])
    validate_data(tr, vschema)


def test_pred_nonvalid_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat': 'a', 'ColBool':True},
        {'ColInt': 'jello', 'ColFloat':None, 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColInt' in testrows[1])
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert not('ColInt' in tr[1])
    validate_predictions(tr, vschema)

# Non-float Real
def test_data_non_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': '4.1', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_pred_non_float_real_fail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': '4.1', 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_non_float_real_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': '4.1', 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColFloat'] == 4.1
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert tr[1]['ColFloat'] == 4.1
    validate_data(tr, vschema)


def test_pred_non_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': '4.1', 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColFloat'] == 4.1
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert tr[1]['ColFloat'] == 4.1
    validate_predictions(tr, vschema)


# Non-valid-float Real
def test_data_nonvalid_float_real_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_pred_nonvalid_float_real_fail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_nonvalid_float_real_fixfail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema,
        convert_types=True)
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_pred_nonvalid_float_real_fixfail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b', 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema, convert_types=True)
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'


def test_data_nonvalid_float_real_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat': 'jello', 'ColCat': 'b',
         'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColFloat' in testrows[1])
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert not('ColFloat' in tr[1])
    validate_data(tr, vschema)


def test_pred_nonvalid_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat': 'jello', 'ColCat': 'b', 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColFloat' in testrows[1])
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert not('ColFloat' in tr[1])
    validate_predictions(tr, vschema)


# Non-str Category
def test_data_non_str_cat_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_pred_non_str_cat_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    assert_raises(DataValidationException, validate_predictions, testrows, vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'


def test_data_non_str_cat_fix():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColCat'] == '3'
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert tr[1]['ColCat'] == '3'
    validate_data(tr, vschema)


def test_pred_non_str_cat_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColCat'] == '3'
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert tr[1]['ColCat'] == '3'
    validate_predictions(tr, vschema)


# Non-bool boolean
def test_data_non_bool_boolean_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '0'}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_non_bool_boolean_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': '0'}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
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
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == True
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    for r in tr:
        assert r['ColBool'] == True
    validate_data(tr, vschema)


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
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == True
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    for r in tr:
        assert r['ColBool'] == True
    validate_predictions(tr, vschema)


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
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == False
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    for r in tr:
        assert r['ColBool'] == False
    validate_data(tr, vschema)


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
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == False
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    for r in tr:
        assert r['ColBool'] == False
    validate_predictions(tr, vschema)


# Non-valid-bool boolean
def test_data_nonvalid_bool_boolean_fail():
    testrows = [
        {'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
         'ColBool': 'jello'}]
    assert_raises(DataValidationException, validate_data, testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_nonvalid_bool_boolean_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'jello'}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema)
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_data_nonvalid_bool_boolean_fixfail():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
        'ColBool':True}, {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
        'ColBool': 'jello'}]
    tr = deepcopy(testrows)
    assert_raises(DataValidationException, validate_data, tr, vschema)
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_pred_nonvalid_bool_boolean_fixfail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'jello'}]
    assert_raises(DataValidationException, validate_predictions, testrows,
        vschema, convert_types=True)
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'


def test_data_nonvalid_bool_boolean_fix():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a',
        'ColBool':True}, {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b',
        'ColBool': 'jello'}]
    tr = deepcopy(testrows)
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColBool' in testrows[1])
    validate_data(testrows, vschema)
    clean_data(tr, vschema)
    assert not('ColBool' in tr[1])
    validate_data(tr, vschema)


def test_pred_nonvalid_bool_boolean_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool': 'jello'}]
    tr = deepcopy(testrows)
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColBool' in testrows[1])
    validate_predictions(testrows, vschema)
    clean_predictions(tr, vschema)
    assert not('ColBool' in tr[1])
    validate_predictions(tr, vschema)


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
    assert_raises(DataValidationException, validate_data,
        testrows, eschema, convert_types=True)
    try:
        validate_data(testrows, eschema, convert_types=True)
    except DataValidationException as e:
        assert e.col == 'ColCat'


def test_pred_too_many_cats_fail():
    eschema = {
        'ColCat': {'type': 'categorical'}
    }
    testrows = []
    rid = 0
    maxCols = 256
    for i in range(maxCols - 1):
        testrows.append({'ColCat':str(i)})
        testrows.append({'ColCat':str(i)})
        rid = rid + 2
    testrows.append({'ColCat':str(maxCols - 1)})
    testrows.append({'ColCat':str(maxCols)})
    assert_raises(DataValidationException,
        validate_predictions, testrows, eschema, convert_types=True)
    try:
        validate_predictions(testrows, eschema, convert_types=True)
    except DataValidationException as e:
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
    tr = deepcopy(testrows)
    validate_data(testrows, eschema, reduce_categories=True)
    assert testrows[510]['ColCat'] == 'Other'
    assert testrows[511]['ColCat'] == 'Other'
    validate_data(testrows, eschema)
    clean_data(tr, eschema)
    assert tr[510]['ColCat'] == 'Other'
    assert tr[511]['ColCat'] == 'Other'
    validate_data(tr, eschema)


def test_data_empty_col_fail():
    testrows = [{'_id': '1', 'ColInt':3, 'ColFloat':3.1, 'ColBool':True},
                {'_id': '2', 'ColInt':4, 'ColFloat':4.1, 'ColBool':False}]
    assert_raises(DataValidationException, validate_data,
        testrows, vschema)
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.col == 'ColCat'


class TestSummarize:
    def setup(self):
        self.testpreds = [
            {'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':False},
            {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False},
            {'ColInt':8, 'ColFloat':8.1, 'ColCat': 'b', 'ColBool':False},
            {'ColInt':11, 'ColFloat':2.1, 'ColCat': 'c', 'ColBool':True}]

    def test_summarize_count(self):
        expected, uncertainty = summarize(self.testpreds, 'ColInt')
        assert type(expected) == int
        assert expected == int(round((3 + 4 + 8 + 11) / 4.0))
        assert abs(uncertainty - 3.6968) < 0.001

    def test_summarize_real(self):
        expected, uncertainty = summarize(self.testpreds, 'ColFloat')
        assert type(expected) == float
        assert abs(expected - 4.35) < 0.001
        assert abs(uncertainty - 2.6299) < 0.001

    def test_summarize_cat(self):
        expected, uncertainty = summarize(self.testpreds, 'ColCat')
        assert type(expected) == str
        assert expected == 'b'
        assert abs(uncertainty - 0.5) < 0.001

    def test_summarize_bool(self):
        expected, uncertainty = summarize(self.testpreds, 'ColBool')
        assert type(expected) == bool
        assert expected == False
        assert abs(uncertainty - 0.25) < 0.001
