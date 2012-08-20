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

class TestGroup:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)
        self.t = self.API.create_table()
        self.rows = [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
         {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
         {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True},
         {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True},
         {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False},
         {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}]
        self.t.batch_upload_rows(self.rows)
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

    def _check_grouping(self, g, col):
        assert_equal(g.state, 'succeeded')
        assert_equal(g.column_id, col)

        g = self.a.get_grouping(col)
        assert_equal(g.state, 'succeeded')
        assert_equal(g.column_id, col)

        g = list(self.a.get_groupings([col]))[0]
        assert_equal(g.state, 'succeeded')
        assert_equal(g.column_id, col)

    @attr('async')
    def test_get_grouping(self):
        self.a.wait()
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            self._check_grouping(g, col)
            
    @attr('async')
    def test_get_groupings(self):
        self.a.wait()
        groupings = self.a.get_groupings(self.schema.keys())
        for g in groupings:
            col = g.column_id
            g.wait()
            self._check_grouping(g, col)

    @attr('async')
    def test_grouping_groups(self):
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            groups = g.get_groups()
            for gid in groups:
                group_rows = g.get_rows(gid)
                for row in group_rows:
                    assert_equal(row['_group_id'], gid)

    @attr('async')
    def test_grouping_rows(self):
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            rows = g.get_rows()
            assert_equal(set([r['_id'] for r in self.rows]),
                         set([r['_id'] for r in rows]))

    def _check_row(self, row):
        assert '_id' in row
        assert '_group_id' in row
        assert '_group_confidence' in row

    @attr('async')
    def test_return_data_true(self):
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            rows = g.get_rows()
            table_rows = dict([(r['_id'], r) for r in self.rows])
            for row in rows:
                self._check_row(row)
                for key, val in table_rows[row['_id']].items():
                    assert_equal(row[key], val)

    @attr('async')
    def test_return_data_false(self):
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            rows = g.get_rows(return_data=False)
            table_rows = dict([(r['_id'], r) for r in self.rows])
            for row in rows:
                assert_equal(set(['_id', '_group_id', '_group_confidence']),
                             set(row.keys()))

    @attr('async')
    def test_get_row(self):
        for col in self.schema.keys():
            g = self.a.get_grouping(col)
            g.wait()
            for row in self.rows:
                group_row = g.get_row(row)
                self._check_row(group_row)


