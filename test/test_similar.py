#! usr/bin/python
# coding=utf-8

# NOTE: for py.26 compatibility, comment tests out to skip them -- don't use
# unittest.skip

import veritable
import random
import os
import json
from nose.plugins.attrib import attr
from nose.tools import *
from nose.tools import assert_raises, assert_true, assert_equal
from veritable.exceptions import VeritableError
from veritable.api import Prediction

TEST_API_KEY = os.getenv("VERITABLE_KEY")
TEST_BASE_URL = os.getenv("VERITABLE_URL") or "https://api.priorknowledge.com"
OPTIONS = os.getenv("VERITABLE_NOSETEST_OPTIONS", [])
connect_kwargs = {}
if 'nogzip' in OPTIONS:
    connect_kwargs.update({'enable_gzip': False})
if 'nossl' in OPTIONS:
    connect_kwargs.update({'ssl_verify': False})

INVALID_IDS = ["éléphant", "374.34", "ajfh/d/sfd@#$",
    "\xe3\x81\xb2\xe3\x81\x9f\xe3\x81\xa1\xe3\x81\xae", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar", 3, 1.414, False, True,
    "_underscore"]


class TestSimilar:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)
        self.t = self.API.create_table()
        self.t.batch_upload_rows(
        [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
         {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
         {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True},
         {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True},
         {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False},
         {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}
        ])
        self.schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.a = self.t.create_analysis(self.schema, analysis_id="a1",
            force=True)
        self.a.wait()

    @classmethod
    def teardown_class(self):
        self.t.delete()


    def setup(self):
        pass

    def teardown(self):
        pass

    @attr('async')
    def test_similar_to(self):
        for col in self.schema.keys():
            for row in self.t.get_rows():
                self.a.similar_to({'_id': row['_id']}, col, 1)

    @attr('async')
    def test_similar_to_id_only(self):
        self.a.similar_to('row1', 'cat')

    @attr('async')
    def test_similar_to_with_invalid_column_fails(self):
        assert_raises(VeritableError, list, self.a.similar_to(
          {'_id': 'row1'}, 'missing-col', 1))
        
    @attr('async')
    def test_similar_to_with_invalid_row_fails(self):
        assert_raises(VeritableError, list, self.a.similar_to(
          {'_id': 'missing-row:'}, 'ct', 1))

    @attr('async')
    def test_similar_to_return_data(self):
        result = self.a.similar_to({'_id': 'row1'}, 'cat', 1, return_data=True)
        for row in result:
            assert_true('_id' in row)
            assert_true('_similarity' in row)
            assert_true('ct' in row)
        result = self.a.similar_to({'_id': 'row1'}, 'cat', 1, return_data=False)
        for row in result:
            assert_true('_id' in row)
            assert_true('_similarity' in row)
            assert_false('ct' in row)

    @attr('async')
    def test_similar_to_link_is_present(self):
        self.a._link('similar')

    @attr('async')
    def test_similar_to_result(self):
        assert(len([r for r in self.a.similar_to({'_id': 'row1'}, 'cat', 1)]) <= 1)

    @attr('async')
    def test_similar_to_result_limit_0(self):
        assert(len([r for r in self.a.similar_to({'_id': 'row1'}, 'cat',
            max_rows=0)]) == 0)

    @attr('async')
    def test_similar_to_result_limit_3(self):
        assert(len([r for r in self.a.similar_to({'_id': 'row1'}, 'cat',
            max_rows=3)]) <= 3)
