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



class TestAPI:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)

    @attr('sync')
    def test_print_API(self):
        print(self.API)

    @attr('sync')
    def test_get_tables(self):
        tables = list(self.API.get_tables())
        assert_true(len(tables) > 0)
        for table in tables:
            assert_true(isinstance(table, veritable.api.Table))

    @attr('sync')
    def test_create_table_autoid(self):
        t = self.API.create_table()
        t.delete()

    @attr('sync')
    def test_print_table(self):
        t = self.API.create_table()
        print(t)
        t.delete()

    @attr('sync')
    def test_create_table_with_id(self):
        t = self.API.create_table("foo" + str(random.randint(0, 100000000)),
            force=True)
        t.delete()

    @attr('sync')
    def test_create_table_with_id_json_roundtrip(self):
        id = json.loads(json.dumps(
            {'id': "foo" + str(random.randint(0, 100000000))}))['id']
        t = self.API.create_table(id, force=True)
        t.delete()

    @attr('sync')
    def test_create_table_with_invalid_id(self):
        for id in INVALID_IDS:
            assert_raises(VeritableError, self.API.create_table, id)
        for id in INVALID_IDS:
            if(self.API.table_exists(id)):
                self.API.delete_table(id)

    @attr('sync')
    def test_create_table_with_description(self):
        t = self.API.create_table("foo" + str(random.randint(0, 100000000)),
            description="A table of humbuggery")
        t.delete()

    @attr('sync')
    def test_get_table_by_id_attr(self):
        t = self.API.create_table()
        t = self.API.get_table(t.id)
        t.delete()

    @attr('sync')
    def test_delete_table(self):
        t = self.API.create_table()
        t.delete()

    @attr('sync')
    def test_delete_deleted_table(self):
        t = self.API.create_table()
        t.delete()
        t.delete()

    @attr('sync')
    def test_create_deleted_table(self):
        t = self.API.create_table()
        id = t.id
        t.delete()
        t = self.API.create_table(id)
        t.delete()

    @attr('sync')
    def test_get_deleted_table(self):
        t = self.API.create_table()
        id = t.id
        t.delete()
        assert_raises(VeritableError, self.API.get_table, id)

    @attr('sync')
    def test_create_duplicate_tables(self):
        t = self.API.create_table()
        assert_raises(VeritableError, self.API.create_table, t.id)
        t.delete()

    @attr('sync')
    def test_create_duplicate_tables_force(self):
        t = self.API.create_table()
        t = self.API.create_table(t.id, force=True)
        t.delete()

    @attr('sync')
    def test_delete_table_by_id(self):
        t = self.API.create_table()
        self.API.delete_table(t.id)
