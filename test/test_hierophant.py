#! usr/bin/python

import simplejson as json
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from hierophant.api import veritable_connect, DeletedTableException
from hierophant.utils import format_url

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"

def test_create_api():
	API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

def test_get_tables():
	ts = API.get_tables()

def test_create_table_autoid():
	t1 = API.create_table()[0]

def test_create_table_id():
	t2 = API.create_table("foo")[0]

def test_create_table_desc():
	t3 = API.create_table("bar", "A table of humbuggery")[0]

def test_get_table_by_id():
	t4 = API.get_table_by_id("foo")[0]

def test_delete_table():
	t4.delete()

@raises(DeletedTableException)
def test_get_deleted_table():
	t4.get()

def test_get_table_by_url():
	t5 = API.create_table()[0]
	API.get_table_by_url(t5.links["self"])

@raises(DeletedTableException)
def test_get_deleted_table_by_id():
	API.get_table_by_id("foo")

def test_get_deleted_table_by_url():
	API.get_table_by_url(t4.links["self"])

def test_tables_still_alive():
	ts2 = [t[0] for t in API.get_tables()]
	[t.still_alive() for t in ts2]

def test_create_duplicate_tables():
	t5 = API.create_table("foo")[0]
	t5 = API.create_table("foo")[0]

def test_delete_table_by_url():
	t6 = API.create_table("baz")[0]
	API.delete_table_by_url(t6.links["self"])

def test_delete_table_by_id():
	t7 = API.create_table("buzz")[0]
	API.delete_table_by_id("bazz")

@raises(DeletedTableException)
def test_get_deleted_table_by_url_2():
	API.get_table_by_url(t6.links["self"])

@raises(DeletedTableException)
def test_get_deleted_table_by_url_3():
	API.get_table_by_url(t7.links["self"])

def test_table_add_row_with_id():
	t3.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

def test_table_add_row_with_autogen_id():
	t3.add_row({'zim': 'zom', 'wos': 21.1})

def test_table_add_duplicate_rows():
	t3.add_row({'_id': 'twobug', 'zim': 'vim', 'wos': 11.3})
	t3.add_row({'_id': 'twobug', 'zim': 'fop', 'wos': 17.5})

def test_get_table_data():
	t3.get()

def test_batch_add_rows():
	t3.add_rows([{'_id': 'threebug', 'zim': 'zop', 'wos': 10.3},
				 {'_id': 'fourbug', 'zim': 'zam', 'wos': 9.3},
				 {'_id': 'fivebug', 'zim': 'zop', 'wos': 18.9}])

def test_batch_add_rows_autogen():
	t3.add_rows([{'zim': 'zop', 'wos': 10.3},
				 {'zim': 'zam', 'wos': 9.3},
				 {'zim': 'zop', 'wos': 18.9},
				 {'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])

def test_get_row_by_id():
	t3.get_row_by_id("threebug")

def test_get_row_by_url():
	t3.get_row_by_url(format_url(t3.links["rows"], 'threebug'))

def test_batch_get_rows():
	t3.get_rows()

def test_delete_row_by_id():
	t3.delete_row_by_id("fivebug")

def test_delete_row_by_url():
	t3.delete_row_by_url(format_url(t3.links["rows"], 'threebug'))

@raises(Exception)
def test_delete_deleted_row_by_id():
	t3.delete_row_by_id("fivebug")

@raises(Exception)
def test_delete_deleted_row_by_url():
	t3.delete_row_by_url(format_url(t3.links["rows"], 'threebug'))

def test_batch_delete_rows():
	rs = t3.get_rows
	t3.delete_rows(rs[0:1])
	t3.delete_rows([r["_id"] for r in rs[2:3]])

def test_batch_delete_rows_faulty():
	rs = [{'zim': 'zam', 'wos': 9.3},
	      {'zim': 'zop', 'wos': 18.9}]
	rs.append(t3.get_rows[0])
	t3.delete_rows(rs)

def test_get_analyses():
	t3.get_analyses()

def test_create_analysis():
	pass

def test_create_analysis_faulty_schema():
	pass

def test_create_analysis_faulty_type():
	pass

def test_get_analysis_by_id():
	pass

def test_get_analysis_by_url():
	pass

def test_get_analysis_by_id_fails():
	pass

def test_get_analysis_by_url_fails():
	pass

def test_delete_analysis_by_id():
	pass

def test_delete_analysis_by_url():
	pass

def test_delete_analysis_by_id_fails():
	pass

def test_delete_analysis_by_url_fails():
	pass

def test_create_duplicate_analysis():
	pass

def test_analyses_still_alive():
	pass

def test_get_analysis_schema():
	pass

def test_get_analysis_schema_fails():
	pass

def test_make_prediction():
	pass

def test_make_prediction_fails():
	pass

def test_delete_analysis():
	pass

def test_learn_analysis():
	pass

def test_get_analysis():
	pass

def test_delete_analysis_fails():
	pass

def test_learn_analysis_fails():
	pass

def test_get_anlysis_fails():
	pass

def test_end_to_end():
	mammals_data = json.loads(file("mammals.json"))
	mammals_handle = API.create_table("mammals", "Oldie but goodie")[0]
	mammals_handle.add_rows(mammals_data)
	mammals_handle.delete_row_by_id("dalmatian")
	mammals_handle.get_rows()
