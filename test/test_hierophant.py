#! usr/bin/python

from hierophant.api import veritable_connect

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000/tables"

API = veritable_connect(TEST_API_KEY)
API.connection.BASE_URL = TEST_BASE_URL

