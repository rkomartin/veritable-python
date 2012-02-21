#! usr/bin/python

import time
import simplejson as json
import veritable
import os
from nose.plugins.attrib import attr
from nose.tools import raises
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError
from veritable.exceptions import *
from veritable.utils import _format_url, wait_for_analysis

TEST_API_KEY = os.getenv("VERITABLE_KEY") or "test"
TEST_BASE_URL = os.getenv("VERITABLE_URL") or "https://api.priorknowledge.com"

def test_create_api():
    API = veritable.connect(TEST_API_KEY, TEST_BASE_URL)

class TestAPI:
    def setup(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL)

    @attr('sync')
    def test_get_tables(self):
        ts = self.API.get_tables()

    @attr('sync')
    def test_create_table_autoid(self):
        t = self.API.create_table()

    @attr('sync')
    def test_create_table_with_id(self):
        t = self.API.create_table("foo", force=True)

    @attr('sync')
    def test_create_table_with_description(self):
        t = self.API.create_table("bar", "A table of humbuggery", force=True)

    @attr('sync')
    def test_get_table_by_id(self):
        t = self.API.create_table("hoo", force=True)
        t = self.API.get_table_by_id("hoo")

    @attr('sync')
    def test_get_table_by_id_2(self):
        t = self.API.create_table("hoo", force=True)
        t = self.API.get_table_by_id(t.id)

    @attr('sync')
    def test_delete_table(self):
        t = self.API.create_table("woo", force=True)
        t = self.API.get_table_by_id("woo")
        t.delete()

    @attr('sync')
    @raises(DeletedTableException)
    def test_get_deleted_table(self):
        t = self.API.create_table("humm", force=True)
        t.delete()
        t._get_state()

    @attr('sync')
    def test_create_deleted_table(self):
        t = self.API.create_table("pumm", force=True)
        t.delete()
        t = self.API.create_table("pumm")

    @attr('sync')
    def test_get_table_by_url(self):
        t = self.API.create_table()
        self.API.get_table_by_url(t.links["self"])

    @attr('sync')
    @raises(ServerException)
    def test_get_deleted_table_by_id(self):
        t = self.API.create_table("rum", force=True)
        t.delete()
        self.API.get_table_by_id("rum")

    @attr('sync')
    @raises(ServerException)
    def test_get_deleted_table_by_url(self):
        t = self.API.create_table()
        t.delete()
        self.API.get_table_by_url(t.links["self"])

    @attr('sync')
    def test_tables_still_alive(self):
        self.API.create_table()
        self.API.create_table()
        self.API.create_table()
        ts = self.API.get_tables()
        [t.still_alive() for t in ts]

    @attr('sync')
    @raises(DuplicateTableException)
    def test_create_duplicate_tables(self):
        t = self.API.create_table("dumb", force=True)
        t = self.API.create_table("dumb")

    @attr('sync')
    def test_create_duplicate_tables_force(self):
        t = self.API.create_table("grimble", force=True)
        t = self.API.create_table("grimble", force=True)

    @attr('sync')
    def test_delete_table_by_url(self):
        t = self.API.create_table("grumble", force=True)
        self.API.delete_table_by_url(t.links["self"])

    @attr('sync')
    def test_delete_table_by_id(self):
        t = self.API.create_table("gramble", force=True)
        self.API.delete_table_by_id("gramble")

    @attr('sync')
    @raises(ServerException)
    def test_delete_table_by_url_then_get_by_url(self):
        t = self.API.create_table("bramble", force=True)
        self.API.delete_table_by_url(t.links["self"])
        self.API.get_table_by_url(t.links["self"])

    @attr('sync')
    @raises(ServerException)
    def test_delete_table_by_id_then_get_by_url(self):
        t = self.API.create_table("fumble", force=True)
        self.API.delete_table_by_id("fumble")
        self.API.get_table_by_url(t.links["self"])

    @attr('sync')
    @raises(ServerException)
    def test_delete_table_by_url_then_get_by_id(self):
        t = self.API.create_table("dapple", force=True)
        self.API.delete_table_by_url(t.links["self"])
        self.API.get_table_by_id("dapple")

    @attr('sync')
    @raises(ServerException)
    def test_delete_table_by_url_then_get_by_url(self):
        t = self.API.create_table("dapple", force=True)
        self.API.delete_table_by_url(t.links["self"])
        self.API.get_table_by_url(t.links["self"])

    @attr('sync')
    def test_table_add_row_with_id(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_add_row_with_int_id_as_string(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': '3', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_add_row_with_float_id_as_string(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': '3.131455', 'zim': 'zam', 'wos': 19.2})

    @raises(TypeError)
    @attr('sync')
    def test_table_add_row_with_int_id(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': 3, 'zim': 'zam', 'wos': 19.2})

    @raises(TypeError)
    @attr('sync')
    def test_table_add_row_with_float_id(self):
        t = self.API.create_table("bugz", force=True)
        t.add_row({'_id': 3.131455, 'zim': 'zam', 'wos': 19.2})


    # This should fail per https://app.asana.com/0/401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    @raises(MissingRowIDException)
    def test_table_add_row_with_autogen_id(self):
        t = self.API.create_table("bugz_2", force=True)
        t.add_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})
        t.add_row({'zim': 'zom', 'wos': 21.1})

    # Should pass silently
    # It's now up to the user to check they aren't overwriting rows
    @attr('sync')
    def test_table_add_duplicate_rows(self):
        t = self.API.create_table("bugz_3", force=True)
        t.add_row({'_id': 'twobug', 'zim': 'vim', 'wos': 11.3})
        t.add_row({'_id': 'twobug', 'zim': 'fop', 'wos': 17.5})

    @attr('sync')
    def test_get_table_state(self):
        t = self.API.create_table("bugz_4", force=True)
        t.add_row({'_id': 'fourbug', 'zim': 'fop', 'wos': 17.5})
        t._get_state()

    @attr('sync')
    def test_batch_add_rows(self):
        t = self.API.create_table("bugz_5", force=True)
        t.add_rows([{'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
                    {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
                    {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])

    # This should fail per https://app.asana.com/0/401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    @raises(MissingRowIDException)
    def test_batch_add_rows_with_missing_ids_fails(self):
        t = self.API.create_table("bugz_6", force=True)
        t.add_rows([{'zim': 'zop', 'wos': 10.3},
                    {'zim': 'zam', 'wos': 9.3},
                    {'zim': 'zop', 'wos': 18.9},
                    {'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])

class TestTableOps:
    def setup(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL)
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

    @attr('sync')
    def test_get_row_by_id(self):
        self.t.get_row_by_id("sixbug")

    @attr('sync')
    def test_get_multiple_rows_by_id(self):
        self.t.get_row_by_id("sixbug")
        self.t.get_row_by_id("fivebug")

    @attr('sync')
    def test_table_add_duplicate_rows_succeeded(self):
        self.t.add_row({'_id': 'threebug', 'zim': 'fop', 'wos': 17.5})
        self.t.add_row({'_id': 'threebug', 'zim': 'vim', 'wos': 11.3})
        assert self.t.get_row_by_id("threebug") == {'_id': 'threebug', 'zim': 'vim', 'wos': 11.3}

    @attr('sync')
    def test_get_row_by_url(self):
        self.t.get_row_by_url(_format_url(self.t.links["rows"], 'fivebug'))

    @attr('sync')
    def test_batch_get_rows(self):
        self.t.get_rows()

    @attr('sync')
    def test_delete_row_by_id(self):
        self.t.delete_row_by_id("fivebug")

    @attr('sync')
    def test_delete_row_by_url(self):
        self.t.delete_row_by_url(_format_url(self.t.links["rows"], 'fourbug'))

    @attr('sync')
    @raises(ServerException)
    def test_delete_deleted_row_by_id(self):
        self.t.delete_row_by_id("fivebug")
        self.t.delete_row_by_id("fivebug")

    @attr('sync')
    @raises(ServerException)
    def test_delete_deleted_row_by_url(self):
        self.t.delete_row_by_id("fivebug")
        self.t.delete_row_by_url(_format_url(self.t.links["rows"], 'fivebug'))

    @attr('sync')
    def test_batch_delete_rows(self):
        rs = self.t.get_rows()
        self.t.delete_rows(rs)
    
    @attr('sync')
    def test_batch_delete_rows_by_id_only(self):
        rs = self.t.get_rows()
        self.t.delete_rows([{'_id': r["_id"]} for r in rs])
    
    @attr('sync')
    @raises(MissingRowIDException)
    def test_batch_delete_rows_faulty(self):
        rs = [{'zim': 'zam', 'wos': 9.3},
              {'zim': 'zop', 'wos': 18.9}]
        rs.append(self.t.get_rows())
        self.t.delete_rows(rs)            

    @attr('sync')
    def test_get_analyses(self):
        self.t.get_analyses()

    @attr('sync')
    def test_create_analysis_1(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_1", force=True)

    @attr('sync')
    def test_create_analysis_2(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, description="Foolish", analysis_id="zubble_2", force=True)

    @attr('sync')
    def test_create_analysis_autogen_id(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema)

    @attr('sync')
    @raises(DuplicateAnalysisException)
    def test_create_duplicate_analysis(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_2", force=True)
        self.t.create_analysis(schema, analysis_id="zubble_2")

    @attr('sync')
    def test_create_duplicate_analysis_force(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_3", force=True)
        self.t.create_analysis(schema, analysis_id="zubble_3", force=True)

    @attr('async')
    def test_create_analysis_malformed_schema_datatype_column_mismatch(self):
        schema = {'zim': {'type': 'real'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    # Unpossible datatypes are identified synchronously
    @attr('sync')
    @raises(ServerException)
    def test_create_analysis_malformed_schema_unpossible_datatypes(self):
        schema = {'zim': {'type': 'generalized_wishart_process'}, 'wos': {'type': 'ibp'}}
        a = self.t.create_analysis(schema)

    @attr('sync')
    @raises(ServerException)
    def test_create_analysis_malformed_schema_2(self):
        schema = 'wimmel'
        a = self.t.create_analysis(schema)

    @attr('sync')
    @raises(ServerException)
    def test_create_analysis_malformed_schema_3(self):
        schema = {}
        a = self.t.create_analysis(schema)

    @attr('sync')
    @raises(ServerException)
    def test_create_analysis_malformed_schema_4(self):
        schema = {'zim': 'real', 'wos': 'real'}
        a = self.t.create_analysis(schema)

    @attr('sync')
    @raises(ServerException)
    def test_create_analysis_malformed_schema_5(self):
        schema = ['categorical', 'real']
        a = self.t.create_analysis(schema)

    # Unpossible analysis types are identified synchronously
    @attr('sync')
    @raises(InvalidAnalysisTypeException)
    def test_create_analysis_faulty_type(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, type="svm", analysis_id="zubble_2")

    @attr('async')
    def test_wait_for_analysis_succeeds(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        wait_for_analysis(a)
        assert a.status() == "succeeded"

    @attr('async')
    def test_wait_for_analysis_fails(self):
        schema = {'zim': {'type': 'boolean'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_error_analysis_failed(self):
        schema = {'zim': {'type': 'boolean'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        wait_for_analysis(a)
        a.error

    # This test should not error synchronously -- it should fail only after
    # scanning the data and finding no rows with column "krob"
    @attr('async')
    def test_create_analysis_malformed_schema_6(self):
        schema = {'zim': {'type': 'categorical'},
                  'wos': {'type': 'real'},
                  'krob': {'type': 'count'}}
        a = self.t.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('sync')
    def test_create_analysis_with_all_datatypes(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="test_analysis", force=True)
        wait_for_analysis(a)        
        assert a.status() == "succeeded"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_real(self):
        schema = {'cat': {'type': 'real'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_count(self):
        schema = {'cat': {'type': 'count'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_boolean(self):
        schema = {'cat': {'type': 'boolean'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_boolean_real(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'real'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_boolean_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'count'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'count'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'categorical'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'boolean'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_count_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'boolean'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_count_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'categorical'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        wait_for_analysis(a)
        assert a.status() == "failed"

    @attr('sync')
    def test_get_analysis_by_id(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="test_analysis", force=True)
        self.t2.get_analysis_by_id("test_analysis")

    @attr('sync')
    def test_get_analysis_by_url(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="test_analysis", force=True)
        self.t2.get_analysis_by_url(a.links["self"])
      
    @attr('sync')
    @raises(ServerException)
    def test_get_analysis_by_id_fails(self):
        self.t2.get_analysis_by_id("yummy_tummy")

    @attr('sync')
    @raises(ServerException)
    def test_get_analysis_by_url_fails(self):
        self.t2.get_analysis_by_url("grubble")

    @attr('sync')
    def test_delete_analysis_by_id(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="delete_me", force=True)
        self.t2.delete_analysis_by_id("delete_me")

    @attr('sync')
    def test_delete_analysis_by_url(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="delete_me", force=True)
        self.t2.delete_analysis_by_url(a.links["self"])

    @attr('sync')
    @raises(ServerException)
    def test_delete_analysis_by_id_fails(self):
        self.t2.delete_analysis_by_id("foobar")

    @attr('sync')
    @raises(ServerException)
    def test_delete_analysis_by_url_fails(self):
        self.t2.delete_analysis_by_url("grumble")

    @attr('sync')
    @raises(DuplicateAnalysisException)
    def test_create_duplicate_analysis(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="double", force=True)
        self.t2.create_analysis(schema, analysis_id="double")

    @attr('sync')
    def test_analyses_still_alive(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="a", force=True)
        self.t2.create_analysis(schema, analysis_id="b", force=True)
        self.t2.create_analysis(schema, analysis_id="c", force=True)
        aa = self.t2.get_analyses()
        [a.still_alive() for a in aa]

    @attr('sync')
    def test_get_analysis_schema(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="a", force=True)
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

    def test_predict_link_is_present(self):
        pass
        
    def test_predictions_ready(self):
        pass

    def test_make_too_many_predictions(self):
        pass
