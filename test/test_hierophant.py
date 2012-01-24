#! usr/bin/python

from requests.auth import HTTPBasicAuth
from hierophant.api import veritable_connect, DeletedTableException
from requests.exceptions import HTTPError

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"

def test_create_api():
	API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

def test_get_tables():
	ts = API.get_tables()
	assert len(ts) == 0

def test_create_table_autoid():
	t1 = API.create_table()

def test_create_table_id():
	t2 = API.create_table("foo")

def test_create_table_desc():
	t3 = API.create_table("bar", "A table of humbuggery")

def test_get_table_by_id():
	t4 = API.get_table_by_id("foo")

def test_delete_table():
	t4.delete()

@raises(DeletedTableException)
def test_get_deleted_table():
	t4.get()

def test_tables_created():
	ts2 = API.get_tables()
	assert len(ts2) = 3

def test_get_table_by_url():
	pass

def test_get_deleted_table_by_id():
	pass

def test_get_deleted_table_by_url():
	pass

def test_tables_still_alive():
	[t.still_alive() for t in ts2]

def test_create_duplicate_tables():
	t5 = API.create_table("foo")
	t5 = API.create_table("foo")

def test_delete_table_by_url():
	pass

def test_delete_table_by_id():
	pass

def test_get_deleted_table_by_url_2():
	pass

def test_get_deleted_table_by_url_3():
	pass

def test_table_add_row_with_id():
	pass

def test_table_add_row_with_autogen_id():
	pass

def test_table_add_duplicate_rows():
	pass

def test_get_table_data():
	pass

def test_batch_add_rows():
	pass

def test_batch_add_rows_autogen():
	pass

def test_get_row_by_id():
	pass

def test_get_row_by_url():
	pass

def test_batch_get_rows():
	pass

def test_delete_row_by_id():
	pass

def test_delete_row_by_url():
	pass

def test_batch_delete_rows():
	pass

def test_batch_delete_rows_faulty():
	pass

def test_get_analyses():
	pass

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

def test_get_anlysis():
	pass

def test_delete_analysis_fails():
	pass

def test_learn_analysis_fails():
	pass

def test_get_anlysis_fails():
	pass



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