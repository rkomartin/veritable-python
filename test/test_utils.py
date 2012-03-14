#! usr/bin/python
# coding=utf-8

from veritable.utils import (write_csv, read_csv, make_schema, summarize,
    validate_data, validate_predictions)
from veritable.exceptions import DataValidationException
from nose.tools import raises, assert_raises
from tempfile import mkstemp
import csv
import os

INVALID_IDS = ["éléphant", "374.34", "ajfh/d/sfd@#$", u"ひたちの", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar"]


def test_write_read_csv():
    handle, filename = mkstemp()
    refrows = [{'_id':'7', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a'},
               {'_id':'8', 'ColInt':4, 'ColCat':'b', 'ColBool':False},
               {'_id':'9'}]
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
    os.remove(filename)


def test_read_csv_map_id():
    handle, filename = mkstemp()
    refrows = [
        {'myID':'7', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a',
         'ColBool':True},
        {'myID':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False},
        {'myID':'9'}]
    write_csv(refrows, filename, dialect=csv.excel)
    testrows = read_csv(filename, id_col='myID', dialect=csv.excel)
    cschema = {
        'ColInt':{'type':'count'},
        'ColFloat':{'type':'real'},
        'ColCat':{'type':'categorical'},
        'ColBool':{'type':'boolean'}
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
    refrows = [{'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
                {}]
    write_csv(refrows, filename, dialect=csv.excel)
    testrows = read_csv(filename, dialect=csv.excel)
    cschema = {
        'ColInt':{'type':'count'},
        'ColFloat':{'type':'real'},
        'ColCat':{'type':'categorical'},
        'ColBool':{'type':'boolean'}
        }
    validate_data(testrows, cschema, convert_types=True)
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
    headers = ['IntA','IntB','CatA','CatB','Foo']
    schemaRule = [['Int.*', {'type':'count'}],
                  ['Cat.*', {'type':'categorical'}]  ]
    schema = make_schema(schemaRule, headers=headers)
    assert schema == refSchema


def test_make_schema_rows():
    refSchema = {'CatA': {'type': 'categorical'},
                    'CatB': {'type': 'categorical'},
                    'IntA': {'type': 'count'},
                    'IntB': {'type': 'count'}}
    rows = [{'CatA':None, 'CatB':None, 'IntA':None, 'IntB':None, 'Foo':None}]
    schemaRule = [['Int.*',{'type':'count'}],
                  ['Cat.*',{'type':'categorical'}]]
    schema = make_schema(schemaRule, rows=rows)
    assert schema == refSchema


@raises(Exception)
def test_make_schema_noarg_fail():
    schemaRule = [['Int.*',{'type':'count'}],
                  ['Cat.*',{'type':'categorical'}]]
    make_schema(schemaRule)


# Invalid Schema
@raises(DataValidationException)
def test_missing_schema_type_fail():
    bschema = {'ColInt':{},'ColFloat':{'type':'real'}}
    testrows = []
    validate_data(testrows, bschema)


@raises(DataValidationException)
def test_bad_schema_type_fail():
    bschema = {'ColInt':{'type':'jello'},'ColFloat':{'type':'real'}}
    testrows = []
    validate_data(testrows, bschema)


vschema = {
    'ColInt':{'type':'count'},
    'ColFloat':{'type':'real'},
    'ColCat':{'type':'categorical'},
    'ColBool':{'type':'boolean'}
    }


# Valid
def test_data_valid_rows():
    refrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False},
        {'_id':'3'}]
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False},
        {'_id':'3'}]
    validate_data(testrows, vschema)
    assert testrows == refrows


def test_pred_valid_rows():
    refrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    validate_predictions(testrows, vschema)
    assert testrows == refrows


def test_data_valid_rows_fix():
    refrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False},
        {'_id':'3'}]
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False},
        {'_id':'3'}]
    validate_data(testrows, vschema, convert_types=True, remove_nones=True,
        remove_invalids=True, reduce_categories=True,
        remove_extra_fields=True)
    assert testrows == refrows


def test_pred_valid_rows_fix():
    refrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    testrows = [
        {'ColInt':None, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':None, 'ColBool':False},
        {'ColInt':None, 'ColFloat':None}]
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True, remove_extra_fields=True)
    assert testrows == refrows


# Missing ID
@raises(DataValidationException)
def test_data_missing_id_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        raise


def test_data_missing_id_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate_data(testrows, vschema, assign_ids=True)
    assert testrows[0]['_id'] != testrows[1]['_id']
    validate_data(testrows, vschema)


# Duplicate ID
@raises(DataValidationException)
def test_data_duplicate_id_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'1', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'
        raise


# Non-string ID
@raises(DataValidationException)
def test_data_nonstring_id_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'
        raise


def test_data_nonstring_id_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':2, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['_id'] == '2'
    validate_data(testrows, vschema)


def test_data_nonalphanumeric_ids_fail():
    for id in INVALID_IDS:
        testrows = [
            {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a',
             'ColBool':True},
            {'_id': id, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
             'ColBool':False}]
        assert_raises(DataValidationException, validate_data, testrows,
            vschema)


# Extra Field Not In Schema
def test_data_extrafield_pass():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema)
    assert testrows[1]['ColEx'] == 4


@raises(DataValidationException)
def test_pred_extrafield_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColEx':None, 'ColInt':4, 'ColFloat':None, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColEx'
        raise


@raises(DataValidationException)
def test_pred_idfield_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':None, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == '_id'
        raise


def test_data_extrafield_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColEx':4, 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema, remove_extra_fields=True)
    assert not('ColEx' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_extrafield_fix():
    testrows = [
        {'_id':'1','ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColEx':None, 'ColInt':4, 'ColFloat':None, 'ColCat':'b',
         'ColBool':False}]
    validate_predictions(testrows, vschema, remove_extra_fields=True)
    assert not('_id' in testrows[0])
    assert not('ColEx' in testrows[1])
    validate_predictions(testrows, vschema)


# Field value is None
@raises(DataValidationException)
def test_data_nonefield_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'
        raise


def test_data_nonefield_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':None,
         'ColBool':False}]
    validate_data(testrows, vschema, remove_nones=True)
    assert not('ColCat' in testrows[1])
    validate_data(testrows, vschema)


# Non-int Count
@raises(DataValidationException)
def test_data_non_int_count_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':'4', 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


@raises(DataValidationException)
def test_pred_non_int_count_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':'4', 'ColFloat':None, 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


def test_data_non_int_count_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':'4', 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColInt'] == 4
    validate_data(testrows, vschema)


def test_pred_non_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':'4', 'ColFloat':None, 'ColCat':'b', 'ColBool':False}]
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColInt'] == 4
    validate_predictions(testrows, vschema)


# Non-valid-int Count
@raises(DataValidationException)
def test_data_nonvalid_int_count_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_int_count_fail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':'jello', 'ColFloat':None, 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


@raises(DataValidationException)
def test_data_nonvalid_int_count_fixfail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_int_count_fixfail():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':'jello', 'ColFloat':None, 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColInt'
        raise


def test_data_nonvalid_int_count_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':'jello', 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColInt' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_int_count_fix():
    testrows = [
        {'ColInt':3, 'ColFloat':None, 'ColCat':'a', 'ColBool':True},
        {'ColInt':'jello', 'ColFloat':None, 'ColCat':'b', 'ColBool':False}]
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColInt' in testrows[1])
    validate_predictions(testrows, vschema)


# Non-float Real
@raises(DataValidationException)
def test_data_non_float_real_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':'4.1', 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


@raises(DataValidationException)
def test_pred_non_float_real_fail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':'4.1', 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


def test_data_non_float_real_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':'4.1', 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColFloat'] == 4.1
    validate_data(testrows, vschema)


def test_pred_non_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':'4.1', 'ColCat':'b', 'ColBool':False}]
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColFloat'] == 4.1
    validate_predictions(testrows, vschema)


# Non-valid-float Real
@raises(DataValidationException)
def test_data_nonvalid_float_real_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_float_real_fail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


@raises(DataValidationException)
def test_data_nonvalid_float_real_fixfail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b',
         'ColBool':False}]
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_float_real_fixfail():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColFloat'
        raise


def test_data_nonvalid_float_real_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':'jello', 'ColCat':'b',
         'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColFloat' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_float_real_fix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':'jello', 'ColCat':'b', 'ColBool':False}]
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColFloat' in testrows[1])
    validate_predictions(testrows, vschema)


# Non-str Category
@raises(DataValidationException)
def test_data_non_str_cat_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'
        raise


@raises(DataValidationException)
def test_pred_non_str_cat_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColCat'
        raise


def test_data_non_str_cat_fix():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    validate_data(testrows, vschema, convert_types=True)
    assert testrows[1]['ColCat'] == '3'
    validate_data(testrows, vschema)


def test_pred_non_str_cat_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':3, 'ColBool':False}]
    validate_predictions(testrows, vschema, convert_types=True)
    assert testrows[1]['ColCat'] == '3'
    validate_predictions(testrows, vschema)


# Non-bool boolean
@raises(DataValidationException)
def test_data_non_bool_boolean_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


@raises(DataValidationException)
def test_pred_non_bool_boolean_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
                {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


def test_data_non_bool_boolean_truefix():
    testrows = [
    {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
    {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'1'},
    {'_id':'4', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'2'},
    {'_id':'5', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'True'},
    {'_id':'6', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'true'},
    {'_id':'7', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Yes'},
    {'_id':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'YES'},
    {'_id':'9', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Y'},
    {'_id':'10', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'y'}]
    validate_data(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == True
    validate_data(testrows, vschema)


def test_pred_non_bool_boolean_truefix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'1'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'2'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'True'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'true'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Yes'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'YES'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'Y'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'y'}]
    validate_predictions(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == True
    validate_predictions(testrows, vschema)


def test_data_non_bool_boolean_falsefix():
    testrows = [
    {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':False},
    {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'},
    {'_id':'5', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'False'},
    {'_id':'6', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'false'},
    {'_id':'7', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'No'},
    {'_id':'8', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'NO'},
    {'_id':'9', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'N'},
    {'_id':'10', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'n'}]
    validate_data(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == False
    validate_data(testrows, vschema)


def test_pred_non_bool_boolean_falsefix():
    testrows = [
        {'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':False},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'0'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'False'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'false'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'No'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'NO'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'N'},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'n'}]
    validate_predictions(testrows, vschema, convert_types=True)
    for r in testrows:
        assert r['ColBool'] == False
    validate_predictions(testrows, vschema)


# Non-valid-bool boolean
@raises(DataValidationException)
def test_data_nonvalid_bool_boolean_fail():
    testrows = [
        {'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
         'ColBool':'jello'}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_bool_boolean_fail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    try:
        validate_predictions(testrows, vschema)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


@raises(DataValidationException)
def test_data_nonvalid_bool_boolean_fixfail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a',
        'ColBool':True}, {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
        'ColBool':'jello'}]
    try:
        validate_data(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


@raises(DataValidationException)
def test_pred_nonvalid_bool_boolean_fixfail():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    try:
        validate_predictions(testrows, vschema, convert_types=True)
    except DataValidationException as e:
        assert e.row == 1
        assert e.col == 'ColBool'
        raise


def test_data_nonvalid_bool_boolean_fix():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColCat':'a',
        'ColBool':True}, {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColCat':'b',
        'ColBool':'jello'}]
    validate_data(testrows, vschema, convert_types=True, remove_invalids=True)
    assert not('ColBool' in testrows[1])
    validate_data(testrows, vschema)


def test_pred_nonvalid_bool_boolean_fix():
    testrows = [{'ColInt':None, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':True},
        {'ColInt':None, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':'jello'}]
    validate_predictions(testrows, vschema, convert_types=True,
        remove_invalids=True)
    assert not('ColBool' in testrows[1])
    validate_predictions(testrows, vschema)


# Too many categories
@raises(DataValidationException)
def test_data_too_many_cats_fail():
    eschema = {
        'ColCat':{'type':'categorical'}
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
    try:
        validate_data(testrows, eschema, convert_types=True)
    except DataValidationException as e:
        assert e.col == 'ColCat'
        raise


@raises(DataValidationException)
def test_pred_too_many_cats_fail():
    eschema = {
        'ColCat':{'type':'categorical'}
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
    try:
        validate_predictions(testrows, eschema, convert_types=True)
    except DataValidationException as e:
        assert e.col == 'ColCat'
        raise


def test_data_too_many_cats_fix():
    eschema = {
        'ColCat':{'type':'categorical'}
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
    validate_data(testrows, eschema, reduce_categories=True)
    assert testrows[510]['ColCat'] == 'Other'
    assert testrows[511]['ColCat'] == 'Other'
    validate_data(testrows, eschema)


@raises(DataValidationException)
def test_data_empty_col_fail():
    testrows = [{'_id':'1', 'ColInt':3, 'ColFloat':3.1, 'ColBool':True},
                {'_id':'2', 'ColInt':4, 'ColFloat':4.1, 'ColBool':False}]
    try:
        validate_data(testrows, vschema)
    except DataValidationException as e:
        assert e.col == 'ColCat'
        raise


class TestSummarize:
    def setup(self):
        self.testpreds = [
            {'ColInt':3, 'ColFloat':3.1, 'ColCat':'a', 'ColBool':False},
            {'ColInt':4, 'ColFloat':4.1, 'ColCat':'b', 'ColBool':False},
            {'ColInt':8, 'ColFloat':8.1, 'ColCat':'b', 'ColBool':False},
            {'ColInt':11, 'ColFloat':2.1, 'ColCat':'c', 'ColBool':True}]

    def test_summarize_count(self):
        expected, uncertainty = summarize(self.testpreds, 'ColInt')
        assert type(expected) == int
        assert expected == 7
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
