#! usr/bin/python

from requests.auth import HTTPBasicAuth
from hierophant.api import veritable_connect, DeletedTableException
from requests.exceptions import HTTPError

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"

API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

ts = API.tables()

t1 = API.create_table()
t2 = API.create_table("foo")
t3 = API.create_table("bar", "A table of humbuggery")
t4 = API.get_table_by_id("foo")
t4.delete()

try:
	t4.get()
except DeletedTableException:
	pass

try:
	t4 = API.get_table_by_id("foo")
except HTTPError:
	pass

ts2 = API.tables()

[t.still_alive() for t in ts2]

t5 = API.create_table("foo")
t5 = API.create_table("foo")

t5.add_row({'furry': 'true', 'hooves': 'false', 'size': 2})
t5.add_row({'_id': 'camel', 'furry': 'true', 'hooves': 'true', 'size': 4})
t5.add_row({'_id': 'camel', 'furry': 'true', 'hooves': 'true', 'size': 4})
t5.add_row({'_id': 'lizard', 'furry': 'false', 'hooves': 'false', 'size': 1})
t5.get_rows()
t5.get_row('camel')
t5.get_row('lizard')
t5.delete_row('lizard')
t5.delete_row('lizard')
t5.get_row('lizard')
t5.delete_row('camel')
t5.get_rows()

t6 = t5.get()
t5.last_updated == t6.last_updated

#add_rows
#get - compare timestamps
#create_analysis
#get_analyses

# analyses - get, learn, delete, get_schema, predict