#! usr/bin/python

from results.auth import HTTPBasicAuth
from hierophant.api import veritable_connect
from hierophant.utils import join_url, make_table_id

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000/tables"
TEST_AUTH = HTTPBasicAuth(TEST_API_KEY, "")

API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

API.connection.BASE_URL = TEST_BASE_URL

ts1 = API.connection.get()
ts2 = API.connection.get(TEST_BASE_URL)

t1 = API.connection.put(join_url(TEST_BASE_URL, make_table_id()), auth=TEST_AUTH)
t2 = API.connection.put(join_url(TEST_BASE_URL, "gargh"), auth=TEST_AUTH)
t3 = API.connection.put(join_url(TEST_BASE_URL, "zim"),
				   data={'description': "A table of foolishness"}, auth=TEST_AUTH)

t4 = API.connection.get(join_url(TEST_BASE_URL, "gargh"))
t5 = API.connection.delete(join_url(TEST_BASE_URL, "gargh"))
t4 = API.connection.get(join_url(TEST_BASE_URL, "gargh"))

#t4 = Table(API.connection, t1)
#t5 = Table(API.connection, t2)
#t6 = Table(API.connection, t3)

#t4.still_alive()
#t5.still_alive()
#t6.still_alive()

#t4.get()
#t5.get()
#t6.get()

ts = API.tables()

t1 = API.create_table()
#t2 = API.create_table("foo")
#t3 = API.create_table("bar", "A table of humbuggery")

#t4 = API.get_table_by_id("foo")
#t4.delete()
#t4.get()
#t4 = API.get_table_by_id("foo")
