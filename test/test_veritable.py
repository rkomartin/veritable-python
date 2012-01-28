#! usr/bin/python

import simplejson as json
from nose.tools import raises
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from veritable.api import veritable_connect
from veritable.exceptions import *
from veritable.utils import format_url

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"


def test_create_api():
    API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

class TestAPI:
    def setup(self):
        self.API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

    def test_get_tables(self):
        ts = self.API.get_tables()

    def test_create_table_autoid(self):
        t = self.API.create_table()

    def test_create_table_id(self):
        t = self.API.create_table("foo", force=True)

    def test_create_table_desc(self):
        t = self.API.create_table("bar", "A table of humbuggery", force=True)

    def test_get_table_by_id(self):
        t = self.API.create_table("hoo", force=True)
        t = self.API.get_table_by_id("hoo")

    def test_delete_table(self):
        t = self.API.create_table("woo", force=True)
        t = self.API.get_table_by_id("woo")
        t.delete()

    @raises(DeletedTableException)
    def test_get_deleted_table(self):
        t = self.API.create_table("humm", force=True)
        t.delete()
        t.get_state()

    def test_create_deleted_table(self):
        t = self.API.create_table("pumm", force=True)
        t.delete()
        t = self.API.create_table("pumm")

    def test_get_table_by_url(self):
        t = self.API.create_table()
        self.API.get_table_by_url(t.links["self"])

    @raises(Exception)
    def test_get_deleted_table_by_id(self):
        t = self.API.create_table("rum", force=True)
        t.delete()
        self.API.get_table_by_id("rum")

    @raises(Exception)
    def test_get_deleted_table_by_url(self):
        t = self.API.create_table()
        t.delete()
        self.API.get_table_by_url(t.links["self"])

    def test_tables_still_alive(self):
        self.API.create_table()
        self.API.create_table()
        self.API.create_table()
        ts = self.API.get_tables()
        [t.still_alive() for t in ts]

    @raises(DuplicateTableException)
    def test_create_duplicate_tables(self):
        t = self.API.create_table("dumb", force=True)
        t = self.API.create_table("dumb")

    def test_create_duplicate_tables_force(self):
        t = self.API.create_table("grimble", force=True)
        t = self.API.create_table("grimble", force=True)

    def test_delete_table_by_url(self):
        t = self.API.create_table("grumble", force=True)
        self.API.delete_table_by_url(t.links["self"])

    def test_delete_table_by_id(self):
        t = self.API.create_table("gramble", force=True)
        self.API.delete_table_by_id("gramble")

    @raises(Exception)
    def test_get_deleted_table_by_url_2(self):
        t = self.API.create_table("bramble", force=True)
        self.API.delete_table_by_url(t.links["self"])
        self.API.get_table_by_url(t.links["self"])

    @raises(Exception)
    def test_get_deleted_table_by_url_3(self):
        t = self.API.create_table("fumble", force=True)
        self.API.delete_table_by_id("fumble")
        self.API.get_table_by_url(t.links["self"])

    def test_table_add_row_with_id(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

    def test_table_add_row_with_autogen_id(self):
        t = self.API.create_table("bugz_2", force=True)
        t.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})
        t.add_row({'zim': 'zom', 'wos': 21.1})

    @raises(DuplicateRowException)
    def test_table_add_duplicate_rows(self):
        t = self.API.create_table("bugz_3", force=True)
        t.add_row({'_id': 'twobug', 'zim': 'vim', 'wos': 11.3})
        t.add_row({'_id': 'twobug', 'zim': 'fop', 'wos': 17.5})

    def test_table_add_duplicate_rows_force(self):
        t = self.API.create_table("bugz_3", force=True)
        t.add_row({'_id': 'threebug', 'zim': 'fop', 'wos': 17.5})
        t.add_row({'_id': 'threebug', 'zim': 'vim', 'wos': 11.3}, force=True)

    def test_get_table_data(self):
        t = self.API.create_table("bugz_4", force=True)
        t.add_row({'_id': 'fourbug', 'zim': 'fop', 'wos': 17.5})
        t.get_state()

    def test_batch_add_rows(self):
        t = self.API.create_table("bugz_5", force=True)
        t.add_rows([{'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
                    {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
                    {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])

    def test_batch_add_rows_autogen(self):
        t = self.API.create_table("bugz_6", force=True)
        t.add_rows([{'zim': 'zop', 'wos': 10.3},
                    {'zim': 'zam', 'wos': 9.3},
                    {'zim': 'zop', 'wos': 18.9},
                    {'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])

class TestTableOps:
    def setup(self):
        self.API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)
        self.t = self.API.create_table(table_id = "bugz", force=True)
        self.t.add_rows([{'_id': 'onebug', 'zim': 'zam', 'wos': 19.2},
                         {'_id': 'twobug', 'zim': 'vim', 'wos': 11.3},
                         {'_id': 'threebug', 'zim': 'fop', 'wos': 17.5},
                         {'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
                         {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
                         {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])
        self.t2 = self.API.create_table(table_id="test_all_types",
                             description="Test dataset with all datatypes",
                             force=True)
        self.t2.add_rows(
          [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
           {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
           {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True},
           {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True},
           {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False},
           {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}
          ])

    def test_get_row_by_id(self):
        self.t.get_row_by_id("sixbug")

    def test_get_multiple_rows_by_id(self):
        self.t.get_row_by_id("sixbug")
        self.t.get_row_by_id("fivebug")

    def test_get_row_by_url(self):
        self.t.get_row_by_url(format_url(t.links["rows"], 'fivebug'))

    def test_batch_get_rows(self):
        self.t.get_rows()

    def test_delete_row_by_id(self):
        self.t.delete_row_by_id("fivebug")

    def test_delete_row_by_url(self):
        self.t.delete_row_by_url(format_url(t.links["rows"], 'fourbug'))

    @raises(Exception)
    def test_delete_deleted_row_by_id(self):
        self.t.delete_row_by_id("fivebug")
        self.t.delete_row_by_id("fivebug")

    @raises(Exception)
    def test_delete_deleted_row_by_url(self):
        self.t.delete_row_by_id("fivebug")
        self.t.delete_row_by_url(format_url(t3.links["rows"], 'fivebug'))

    def test_batch_delete_rows(self):
        rs = self.t.get_rows()["rows"]
        self.t.delete_rows(rs[0:1])
        self.t.delete_rows([{'_id': r["_id"]} for r in rs[2:3]])
    
    @raises(Exception)
    def test_batch_delete_rows_faulty(self):
        rs = [{'zim': 'zam', 'wos': 9.3},
              {'zim': 'zop', 'wos': 18.9}]
        rs.append(self.t.get_rows())
        self.t.delete_rows(rs)            

    def test_get_analyses(self):
        self.t.get_analyses()

    def test_create_analysis_1(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_1")

    def test_create_analysis_2(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, description="Foolish", analysis_id="zubble_2")

    def test_create_analysis_autogen_id(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema)

    @raises(DuplicateAnalysisException)
    def test_create_duplicate_analysis(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_2", force=True)
        self.t.create_analysis(schema, analysis_id="zubble_2")

    def test_create_duplicate_analysis_force(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_3")
        self.t.create_analysis(schema, analysis_id="zubble_3", force=True)

    @raises(Exception)
    def test_create_analysis_faulty_schema_1(self):
        schema = {'zim': {'type': 'real'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_schema_2(self):
        schema = 'wimmel'
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_schema_3(self):
        schema = {}
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_schema_4(self):
        schema = {'zim': 'real', 'wos': 'real'}
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_schema_5(self):
        schema = ['categorical', 'real']
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_schema_6(self):
        schema = {'zim': {'type': 'categorical'},
                  'wos': {'type': 'real'},
                  'krob': {'type': 'count'}}
        self.t.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_faulty_type(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, type="svm", analysis_id="zubble_2")

    def test_create_analysis_with_all_datatypes(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="test_analysis")

    @raises(Exception)
    def test_create_analysis_with_mismatch_categorical_real(self):
        schema = {'cat': {'type': 'real'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_categorical_count(self):
        schema = {'cat': {'type': 'count'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_categorical_boolean(self):
        schema = {'cat': {'type': 'boolean'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_boolean_real(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'real'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_boolean_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'count'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_real_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'count'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_real_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'categorical'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_real_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'boolean'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_count_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'boolean'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    @raises(Exception)
    def test_create_analysis_with_mismatch_count_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'categorical'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema)

    def test_get_analysis_by_id(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="test_analysis")
        self.t2.get_analysis_by_id("test_analysis")

    def test_get_analysis_by_url(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="test_analysis")
        self.t2.get_analysis_by_url(a.links["self"])
      
    @raises(Exception)
    def test_get_analysis_by_id_fails(self):
        self.t2.get_analysis_by_id("yummy_tummy")

    @raises(Exception)
    def test_get_analysis_by_url_fails(self):
        self.t2.get_analysis_by_url("grubble")

    def test_delete_analysis_by_id(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="delete_me")
        self.t2.delete_analysis_by_id("delete_me")

    def test_delete_analysis_by_url(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="delete_me")
        self.t2.delete_analysis_by_url(a.links["self"])

    @raises(Exception)
    def test_delete_analysis_by_id_fails(self):
        self.t2.delete_analysis_by_id("foobar")

    @raises(Exception)
    def test_delete_analysis_by_url_fails(self):
        self.t2.delete_analysis_by_url("grumble")

    @raises(DuplicateAnalysisException)
    def test_create_duplicate_analysis(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="double")
        self.t2.create_analysis(schema, analysis_id="double")

    def test_analyses_still_alive(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="a")
        self.t2.create_analysis(schema, analysis_id="b")
        self.t2.create_analysis(schema, analysis_id="c")
        aa = self.t2.get_analyses()
        [a.still_alive() for a in aa]

    def test_get_analysis_schema(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="a")
        assert schema == a.get_schema()

    # def test_get_analysis_schema_fails(self):
    #     pass

    # def test_make_prediction(self):
    #     pass

    # def test_make_prediction_fails(self):
    #     pass

    # def test_delete_analysis(self):
    #     pass

    # def test_learn_analysis(self):
    #     pass

    # def test_get_analysis(self):
    #     pass

    # def test_delete_analysis_fails(self):
    #     pass

    # def test_learn_analysis_fails(self):
    #     pass

    # def test_get_anlysis_fails(self):
    #     pass

# def test_end_to_end():
#     mammals_data = json.loads(file("mammals.json"))
#     mammals_handle = API.create_table("mammals", "Oldie but goodie")
#     mammals_handle.add_rows(mammals_data)
#     mammals_handle.delete_row_by_id("dalmatian")
#     mammals_handle.get_rows()
