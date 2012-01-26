#! usr/bin/python

import simplejson as json
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from veritable.api import veritable_connect
from veritable.exceptions import *
from veritable.utils import format_url

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"

def test_create_api():
	API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

def test_get_tables():
	ts = API.get_tables()

def test_create_table_autoid():
	t = API.create_table()[0]

def test_create_table_id():
	t = API.create_table("foo")[0]

def test_create_table_desc():
	t = API.create_table("bar", "A table of humbuggery")[0]

def test_get_table_by_id():
	t = API.create_table("hoo")[0]
	t = API.get_table_by_id("hoo")[0]

def test_delete_table():
	t = API.create_table("woo")[0]
	t = API.get_table_by_id("woo")[0]
	t.delete()

@raises(DeletedTableException)
def test_get_deleted_table():
	t = API.create_table("humm")[0]
	t.delete()
	t.get()

def test_create_deleted_table():
	t = API.create_table("pumm")[0]
	t.delete()
	t = API.create_table("pumm")[0]

def test_get_table_by_url():
	t = API.create_table()[0]
	API.get_table_by_url(t.links["self"])

@raises(DeletedTableException)
def test_get_deleted_table_by_id():
	t = API.create_table("rum")[0]
	t.delete()
	API.get_table_by_id("rum")

def test_get_deleted_table_by_url():
	t = API.create_table()[0]
	t.delete()
	API.get_table_by_url(t.links["self"])

def test_tables_still_alive():
	ts = [t[0] for t in API.get_tables()]
	[t.still_alive() for t in ts]

@raises(DuplicateTableException)
def test_create_duplicate_tables():
	t = API.create_table("dumb")[0]
	t = API.create_table("dumb")[0]

def test_create_duplicate_tables_force():
	t = API.create_table("grimble")[0]
	t = API.create_table("grimble", force=True)

def test_delete_table_by_url():
	t = API.create_table("grumble")[0]
	API.delete_table_by_url(t.links["self"])

def test_delete_table_by_id():
	t = API.create_table("gramble")[0]
	API.delete_table_by_id("gramble")

@raises(DeletedTableException)
def test_get_deleted_table_by_url_2():
	t = API.create_table("bramble")[0]
	API.delete_table_by_url(t.links["self"])
	API.get_table_by_url(t.links["self"])

@raises(DeletedTableException)
def test_get_deleted_table_by_url_3():
	t = API.create_table("fumble")[0]
	API.delete_table_by_id("fumble")
	API.get_table_by_url(t.links["self"])

def test_table_add_row_with_id():
	t = API.create_table("bugz")[0]
	t.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

def test_table_add_row_with_autogen_id():
	t = API.get_table_by_id("bugz")[0]
	t.add_row({'zim': 'zom', 'wos': 21.1})

@raises(DuplicateRowException)
def test_table_add_duplicate_rows():
	t = API.get_table_by_id("bugz")[0]
	t.add_row({'_id': 'twobug', 'zim': 'vim', 'wos': 11.3})
	t.add_row({'_id': 'twobug', 'zim': 'fop', 'wos': 17.5})

def test_table_add_duplicate_rows_force():
	t = API.get_table_by_id("bugz")[0]
	t.add_row({'_id': 'threebug', 'zim': 'fop', 'wos': 17.5})
	t.add_row({'_id': 'threebug', 'zim': 'vim', 'wos': 11.3}, force=True)

def test_get_table_data():
	t = API.get_table_by_id("bugz")[0]
	t.get()

def test_batch_add_rows():
	t = API.get_table_by_id("bugz")[0]
	t.add_rows([{'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
				{'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
				{'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])

def test_batch_add_rows_autogen():
	t = API.get_table_by_id("bugz")[0]
	t.add_rows([{'zim': 'zop', 'wos': 10.3},
				{'zim': 'zam', 'wos': 9.3},
				{'zim': 'zop', 'wos': 18.9},
				{'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])

def test_get_row_by_id():
	t = API.get_table_by_id("bugz")[0]
	t.get_row_by_id("threebug")

def test_get_row_by_url():
	t = API.get_table_by_id("bugz")[0]
	t.get_row_by_url(format_url(t.links["rows"], 'threebug'))

def test_batch_get_rows():
	t = API.get_table_by_id("bugz")[0]
	t.get_rows()

def test_delete_row_by_id():
	t = API.get_table_by_id("bugz")[0]
	t.delete_row_by_id("fivebug")

def test_delete_row_by_url():
	t = API.get_table_by_id("bugz")[0]
	t.delete_row_by_url(format_url(t.links["rows"], 'threebug'))

@raises(Exception)
def test_delete_deleted_row_by_id():
	t = API.get_table_by_id("bugz")[0]
	t.delete_row_by_id("fivebug")

@raises(Exception)
def test_delete_deleted_row_by_url():
	t = API.get_table_by_id("bugz")[0]
	t.delete_row_by_url(format_url(t3.links["rows"], 'threebug'))

def test_batch_delete_rows():
	t = API.get_table_by_id("bugz")[0]
	rs = t.get_rows()
	t.delete_rows(rs[0:1])
	t.delete_rows([{'_id': r["_id"]} for r in rs[2:3]])

@raises(Exception)
def test_batch_delete_rows_faulty():
	t = API.get_table_by_id("bugz")[0]
	rs = [{'zim': 'zam', 'wos': 9.3},
	      {'zim': 'zop', 'wos': 18.9}]
	rs.append(t.get_rows[0])
	t.delete_rows(rs)

def test_get_analyses():
	t = API.get_table_by_id("bugz")[0]
	t.get_analyses()

def test_create_analysis_1():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema, analysis_id="zubble_1")

def test_create_analysis_2():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema, description="Foolish", analysis_id="zubble_2")

def test_create_analysis_autogen_id():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema)

@raises(DuplicateAnalysisException)
def test_create_duplicate_analysis():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema, analysis_id="zubble_1")

def test_create_duplicate_analysis_force():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema, analysis_id="zubble_1", force=True)

@raises(Exception)
def test_create_analysis_faulty_schema_1():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'real'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_schema_2():
	t = API.get_table_by_id("bugz")[0]
	schema = 'wimmel'
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_schema_3():
	t = API.get_table_by_id("bugz")[0]
	schema = {}
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_schema_4():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': 'real', 'wos': 'real'}
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_schema_5():
	t = API.get_table_by_id("bugz")[0]
	schema = ['categorical', 'real']
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_schema_6():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'},
			  'wos': {'type': 'real'},
			  'krob': {'type': 'count'}}
	t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_faulty_type():
	t = API.get_table_by_id("bugz")[0]
	schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
	t.create_analysis(schema, type="svm", analysis_id="zubble_2")

# FIXME test each combination of datatypes and column data
def test_create_analysis_with_all_datatypes():
	t = API.create_table(table_id="test_all_types",
						 description="Test dataset with all datatypes")[0]
	t.add_rows(
	  [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
	   {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
	   {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True}
	   {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True}
	   {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False}
	   {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}
	  ])
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
	t.create_analysis(schema, analysis_id="test_analysis")

@raises(Exception)
def test_create_analysis_with_mismatch_categorical_real():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'real'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_categorical_count():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'count'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_categorical_boolean():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'boolean'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_boolean_real():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'real'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_boolean_count():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'count'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_real_count():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'count'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_real_categorical():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'categorical'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_real_boolean():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'boolean'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_count_boolean():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'boolean'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

@raises(Exception)
def test_create_analysis_with_mismatch_count_categorical():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'categorical'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
    t.create_analysis(schema)

def test_get_analysis_by_id():
    t = API.get_table_by_id("test_all_types")[0]
	schema = {'cat': {'type': 'categorical'},
			  'ct': {'type': 'count'},
			  'real': {'type': 'real'},
			  'bool': {'type': 'boolean'}
			  }
	t.create_analysis(schema, analysis_id="test_analysis")
	t.get_analysis_by_id("test_analysis")

def test_get_analysis_by_url():
    t = API.get_table_by_id("test_all_types")[0]
    a = t.get_analysis_by_id("test_analysis")[0]
    t.get_analysis_by_url(a.links["self"])
  
@raises(Exception)
def test_get_analysis_by_id_fails():
    t = API.get_table_by_id("test_all_types")[0]
    t.get_analysis_by_id("yummy_tummy")[0]

@raises(Exception)
def test_get_analysis_by_url_fails():
    t = API.get_table_by_id("test_all_types")[0]
    t.get_analysis_by_url("grubble")[0]

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
