#! usr/bin/python
# coding=utf-8

# NOTE: for py.26 compatibility, comment tests out to skip them -- don't use
# unittest.skip

import veritable
import random
import os
import json
from nose.plugins.attrib import attr
from nose.tools import assert_raises, assert_true, assert_equal
from veritable.exceptions import VeritableError
from veritable.api import Prediction
from veritable.cursor import Cursor

TEST_API_KEY = os.getenv("VERITABLE_KEY")
TEST_BASE_URL = os.getenv("VERITABLE_URL") or "https://api.priorknowledge.com"
OPTIONS = os.getenv("VERITABLE_NOSETEST_OPTIONS", [])
connect_kwargs = {}
if 'nogzip' in OPTIONS:
    connect_kwargs.update({'enable_gzip': False})
if 'nossl' in OPTIONS:
    connect_kwargs.update({'ssl_verify': False})


class TestCursor:
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
        self.connection = self.t._conn
        self.collection = self.t._link("rows")


    @classmethod
    def teardown_class(self):
        self.t.delete()


    def setup(self):
        pass


    def teardown(self):
        pass


    @attr('sync')
    def test_get_cursor_10page(self):
        c = Cursor(self.connection, self.collection, per_page=10, start=None, limit=None)
        assert([r['_id'] for r in list(c)] == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_3page(self):
        c = Cursor(self.connection, self.collection, per_page=3, start=None, limit=None)
        assert([r['_id'] for r in list(c)] == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_4page(self):
        c = Cursor(self.connection, self.collection, per_page=4, start=None, limit=None)
        assert([r['_id'] for r in list(c)] == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_3page_1start(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row1', limit=None)
        assert([r['_id'] for r in list(c)] == ['row1', 'row2', 'row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_3page_2start(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row2', limit=None)
        assert([r['_id'] for r in list(c)] == ['row2', 'row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_3page_3start(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row3', limit=None)
        assert([r['_id'] for r in list(c)] == ['row3', 'row4', 'row5', 'row6'])

    @attr('sync')
    def test_get_cursor_3page_2start_1lim(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row2', limit=1)
        assert([r['_id'] for r in list(c)] == ['row2'])

    @attr('sync')
    def test_get_cursor_3page_2start_2lim(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row2', limit=2)
        assert([r['_id'] for r in list(c)] == ['row2', 'row3'])

    @attr('sync')
    def test_get_cursor_3page_2start_3lim(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row2', limit=3)
        assert([r['_id'] for r in list(c)] == ['row2', 'row3', 'row4'])

    @attr('sync')
    def test_get_cursor_3page_2start_4lim(self):
        c = Cursor(self.connection, self.collection, per_page=3, start='row2', limit=4)
        assert([r['_id'] for r in list(c)] == ['row2', 'row3', 'row4', 'row5'])
