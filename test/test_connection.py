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


class TestConnection:
    def test_create_api(self):
        veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    def test_print_connection(self):
        api = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)
        print(api._conn)

    def test_create_api_with_debug(self):
        veritable.connect(TEST_API_KEY, TEST_BASE_URL, debug=True,
            **connect_kwargs)

    def test_create_api_with_invalid_user(self):
        assert_raises(VeritableError, veritable.connect,
            "completely_invalid_user_id_3426", TEST_BASE_URL,
            **connect_kwargs)

    def test_create_api_with_invalid_server(self):
        assert_raises(VeritableError, veritable.connect,
            "foo", "http://www.google.com", **connect_kwargs)

