#! usr/bin/python

from hierophant.api import veritable_connect

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000/tables"

API = veritable_connect(TEST_API_KEY)
API.connection.BASE_URL = TEST_BASE_URL

API.tables()

t1 = API.create_table()
t2 = API.create_table("foo")
t3 = API.create_table("foo", "A table of humbuggery")
