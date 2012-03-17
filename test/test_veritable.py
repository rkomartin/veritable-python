#! usr/bin/python
# coding=utf-8

import veritable
import unittest
import os
from nose.plugins.attrib import attr
from nose.tools import assert_raises
from requests.exceptions import HTTPError
from veritable.exceptions import *

TEST_API_KEY = os.getenv("VERITABLE_KEY")
TEST_BASE_URL = os.getenv("VERITABLE_URL") or "https://api.priorknowledge.com"
OPTIONS = os.getenv("VERITABLE_NOSETEST_OPTIONS")
connect_kwargs = {}
if 'nogzip' in OPTIONS:
    connect_kwargs.update({'enable_gzip': False})
if 'nossl' in OPTIONS:
    connect_kwargs.update({'ssl_verify': False})

INVALID_IDS = ["éléphant", "374.34", "ajfh/d/sfd@#$", u"ひたちの", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar"]


# FIXME test object representations

class TestConnection:
    def test_create_api(self):
        veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    def test_create_api_with_debug(self):
        veritable.connect(TEST_API_KEY, TEST_BASE_URL, debug=True, **connect_kwargs)

    def test_create_api_with_invalid_user(self):
        assert_raises(HTTPError, veritable.connect,
            "completely_invalid_user_id_3426", TEST_BASE_URL, **connect_kwargs)

    def test_create_api_with_invalid_server(self):
        assert_raises(APIConnectionException, veritable.connect,
            "foo", "http://www.google.com", **connect_kwargs)


class TestAPI:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    @attr('sync')
    def test_get_tables(self):
        self.API.get_tables()

    @attr('sync')
    def test_create_table_autoid(self):
        t = self.API.create_table()
        t.delete()

    @attr('sync')
    def test_create_table_with_id(self):
        t = self.API.create_table("foo", force=True)
        t.delete()

    @attr('sync')
    def test_create_table_with_invalid_id(self):
        for id in INVALID_IDS:
            assert_raises(InvalidIDException, self.API.create_table, id)
        for id in INVALID_IDS:
            if(self.API.table_exists(id)):
                self.API.delete_table(id)

    @attr('sync')
    def test_create_table_with_description(self):
        t = self.API.create_table("bar", "A table of humbuggery", force=True)
        t.delete()

    @attr('sync')
    def test_get_table(self):
        t = self.API.create_table("hoo", force=True)
        t = self.API.get_table("hoo")
        t.delete()

    @attr('sync')
    def test_get_table_by_id_attr(self):
        t = self.API.create_table("hoo", force=True)
        t = self.API.get_table(t.id)
        t.delete()

    @attr('sync')
    def test_delete_table(self):
        t = self.API.create_table("woo", force=True)
        t = self.API.get_table("woo")
        t.delete()

    @attr('sync')
    def test_delete_deleted_table(self):
        t = self.API.create_table("humm", force=True)
        t.delete()
        assert_raises(ServerException, t.delete)

    @attr('sync')
    def test_create_deleted_table(self):
        t = self.API.create_table("pumm", force=True)
        t.delete()
        t = self.API.create_table("pumm")
        t.delete()

    @attr('sync')
    def test_get_deleted_table(self):
        t = self.API.create_table("rum", force=True)
        t.delete()
        assert_raises(ServerException, self.API.get_table, "rum")

    @attr('sync')
    def test_create_duplicate_tables(self):
        t = self.API.create_table("dumb", force=True)
        assert_raises(DuplicateTableException, self.API.create_table, "dumb")
        t.delete()

    @attr('sync')
    def test_create_duplicate_tables_force(self):
        t = self.API.create_table("grimble", force=True)
        t = self.API.create_table("grimble", force=True)
        t.delete()

    @attr('sync')
    def test_delete_table_by_id(self):
        self.API.create_table("gramble", force=True)
        self.API.delete_table("gramble")


class TestRowUploads:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    def setup(self):
        self.t = self.API.create_table("bugz", force=True)

    def teardown(self):
        self.t.delete()

    @attr('sync')
    def test_table_upload_row_with_id(self):
        self.t.upload_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_upload_row_with_invalid_id(self):
        for id in INVALID_IDS:
            assert_raises(InvalidIDException, self.t.upload_row,
                {'_id': id, 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_int_id_as_string(self):
        self.t.upload_row({'_id': '3', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_float_id_as_string(self):
        assert_raises(InvalidIDException, self.t.upload_row,
            {'_id': '3.131455', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_int_id(self):
        assert_raises(TypeError, self.t.upload_row,
            {'_id': 3, 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_float_id(self):
        assert_raises(TypeError, self.t.upload_row,
            {'_id': 3.131455, 'zim': 'zam', 'wos': 19.2})

    # This should fail per 401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    def test_table_upload_row_with_autogen_id(self):
        assert_raises(MissingRowIDException, self.t.upload_row,
            {'zim': 'zom', 'wos': 21.1})

    # Should pass silently
    # It's now up to the user to check they aren't overwriting rows
    @attr('sync')
    def test_table_add_duplicate_rows(self):
        self.t.upload_row({'_id': 'twobug', 'zim': 'vim', 'wos': 11.3})
        self.t.upload_row({'_id': 'twobug', 'zim': 'fop', 'wos': 17.5})

    @attr('sync')
    def test_get_table_state(self):
        self.t.upload_row({'_id': 'fourbug', 'zim': 'fop', 'wos': 17.5})

    @attr('sync')
    def test_batch_upload_rows(self):
        self.t.batch_upload_rows(
            [{'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
             {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
             {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])

    @attr('sync')
    def test_batch_upload_rows_with_invalid_ids(self):
        rows = []
        for id in INVALID_IDS:
            rows = rows + [{'_id': id, 'zim': 'zop', 'wos': 10.3}]
        assert_raises(InvalidIDException, self.t.batch_upload_rows, rows)

    # This should fail per https://app.asana.com/0/401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    def test_batch_upload_rows_with_missing_ids_fails(self):
        assert_raises(MissingRowIDException, self.t.batch_upload_rows,
            [{'zim': 'zop', 'wos': 10.3}, {'zim': 'zam', 'wos': 9.3},
             {'zim': 'zop', 'wos': 18.9},
             {'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])


class TestTableOps:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    def setup(self):
        self.t = self.API.create_table(table_id="bugz", force=True)
        self.t.batch_upload_rows([{'_id': 'onebug', 'zim': 'zam', 'wos': 19.2},
                         {'_id': 'twobug', 'zim': 'vim', 'wos': 11.3},
                         {'_id': 'threebug', 'zim': 'fop', 'wos': 17.5},
                         {'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
                         {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
                         {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])
        self.t2 = self.API.create_table(table_id="test_all_types",
                             description="Test dataset with all datatypes",
                             force=True)
        self.t2.batch_upload_rows(
        [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
         {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
         {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True},
         {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True},
         {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False},
         {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}
        ])

    def teardown(self):
        self.t.delete()
        self.t2.delete()

    @attr('sync')
    def test_get_row(self):
        self.t.get_row("sixbug")

    @attr('sync')
    def test_get_multiple_rows(self):
        self.t.get_row("sixbug")
        self.t.get_row("fivebug")

    @attr('sync')
    def test_table_add_duplicate_rows_succeeded(self):
        self.t.upload_row({'_id': 'threebug', 'zim': 'fop', 'wos': 17.5})
        self.t.upload_row({'_id': 'threebug', 'zim': 'vim', 'wos': 11.3})
        assert self.t.get_row("threebug") == {'_id': 'threebug',
            'zim': 'vim', 'wos': 11.3}

    @attr('sync')
    def test_batch_get_rows(self):
        self.t.get_rows()

# FIXME add tests for paginated get_rows
# FIXME 

    @attr('sync')
    def test_delete_row(self):
        self.t.delete_row("fivebug")

    # This is expected behavior according to the API spec
    @unittest.skip('bug filed')
    @attr('sync')
    def test_delete_deleted_row(self):
        self.t.delete_row("fivebug")
        self.t.delete_row("fivebug")

    @attr('sync')
    def test_batch_delete_rows(self):
        rs = self.t.get_rows()
        self.t.batch_delete_rows(rs)

    @attr('sync')
    def test_batch_delete_rows_by_id_only(self):
        rs = self.t.get_rows()
        self.t.batch_delete_rows([{'_id': r["_id"]} for r in rs])

# FIXME add test for batch_delete_rows_with_some_missing_rows

    @attr('sync')
    def test_batch_delete_rows_faulty(self):
        rs = [{'zim': 'zam', 'wos': 9.3},
              {'zim': 'zop', 'wos': 18.9}]
        rs.append(self.t.get_rows())
        assert_raises(MissingRowIDException, self.t.batch_delete_rows, rs)

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
        self.t.create_analysis(schema, description="Foolish",
            analysis_id="zubble_2", force=True)

    @attr('sync')
    def test_create_analysis_autogen_id(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema)

    @attr('sync')
    def test_create_analysis_with_invalid_id(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        for id in INVALID_IDS:
            assert_raises(InvalidIDException, self.t.create_analysis,
                schema, analysis_id=id)

    @attr('sync')
    def test_create_duplicate_analysis(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_2", force=True)
        assert_raises(DuplicateAnalysisException, self.t.create_analysis,
            schema, analysis_id="zubble_2")

    @attr('sync')
    def test_create_duplicate_analysis_force(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_3", force=True)
        self.t.create_analysis(schema, analysis_id="zubble_3", force=True)

    @attr('async')
    def test_create_analysis_malformed_schema_datatype_column_mismatch(self):
        schema = {'zim': {'type': 'real'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    # Unpossible datatypes are identified synchronously
    @attr('sync')
    def test_create_analysis_malformed_schema_unpossible_datatypes(self):
        schema = {'zim': {'type': 'generalized_wishart_process'},
            'wos': {'type': 'ibp'}}
        assert_raises(ServerException, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_2(self):
        schema = 'wimmel'
        assert_raises(ServerException, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_3(self):
        schema = {}
        assert_raises(ServerException, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_4(self):
        schema = {'zim': 'real', 'wos': 'real'}
        assert_raises(ServerException, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_5(self):
        schema = ['categorical', 'real']
        assert_raises(ServerException, self.t.create_analysis, schema)

    # Unpossible analysis types are identified synchronously
    @attr('sync')
    def test_create_analysis_faulty_type(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        assert_raises(InvalidAnalysisTypeException, self.t.create_analysis,
            schema, type="svm", analysis_id="zubble_2")

    @attr('async')
    def test_wait_for_analysis_succeeds(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        a.wait()
        assert a.state == "succeeded"

    @attr('async')
    def test_wait_for_analysis_fails(self):
        schema = {'zim': {'type': 'boolean'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_error_analysis_failed(self):
        schema = {'zim': {'type': 'boolean'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble")
        a.wait()
        assert(a.error is not None)

    # This test should not error synchronously -- it should fail only after
    # scanning the data and finding no rows with column "krob"
    @attr('async')
    def test_create_analysis_malformed_schema_6(self):
        schema = {'zim': {'type': 'categorical'},
                  'wos': {'type': 'real'},
                  'krob': {'type': 'count'}}
        a = self.t.create_analysis(schema)
        a.wait()
        # TODO: uncomment this check once API is fixed
        # assert a.state == "failed"

    @attr('sync')
    def test_create_analysis_with_all_datatypes(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="test_analysis",
            force=True)
        a.wait()
        assert a.state == "succeeded"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_real(self):
        schema = {'cat': {'type': 'real'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_count(self):
        schema = {'cat': {'type': 'count'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                 }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_categorical_boolean(self):
        schema = {'cat': {'type': 'boolean'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_boolean_real(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'real'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_boolean_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'count'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_count(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'count'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'categorical'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_real_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'boolean'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_count_boolean(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'boolean'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('async')
    def test_create_analysis_with_mismatch_count_categorical(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'categorical'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    @attr('sync')
    def test_get_analyses_after_created(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="test_analysis_1",
            force=True)
        self.t2.create_analysis(schema, analysis_id="test_analysis_2",
            force=True)
        self.t2.get_analyses()

    @attr('sync')
    def test_get_analysis(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="test_analysis",
            force=True)
        self.t2.get_analysis("test_analysis")

    @attr('sync')
    def test_get_analysis_fails(self):
        assert_raises(ServerException, self.t2.get_analysis, "yummy_tummy")

    @attr('sync')
    def test_delete_analysis(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.t2.create_analysis(schema, analysis_id="delete_me", force=True)
        self.t2.delete_analysis("delete_me")

    @attr('sync')
    def test_delete_analysis_fails(self):
        assert_raises(ServerException, self.t2.delete_analysis, "foobar")

    @attr('sync')
    def test_get_analysis_schema(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="a", force=True)
        assert schema == a.get_schema()


class TestPredictions:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL, **connect_kwargs)

    def setup(self):
        self.t = self.API.create_table(table_id="bugz", force=True)
        self.t.batch_upload_rows([{'_id': 'onebug', 'zim': 'zam', 'wos': 19.2},
                         {'_id': 'twobug', 'zim': 'vim', 'wos': 11.3},
                         {'_id': 'threebug', 'zim': 'fop', 'wos': 17.5},
                         {'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
                         {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
                         {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])
        self.schema1 = {'zim': {'type': 'categorical'},
            'wos': {'type': 'real'}}
        self.a1 = self.t.create_analysis(self.schema1, analysis_id="a1",
            force=True)
        self.t2 = self.API.create_table(table_id="test_all_types",
            description="Test dataset with all datatypes", force=True)
        self.t2.batch_upload_rows(
        [{'_id': 'row1', 'cat': 'a', 'ct': 0, 'real': 1.02394, 'bool': True},
         {'_id': 'row2', 'cat': 'b', 'ct': 0, 'real': 0.92131, 'bool': False},
         {'_id': 'row3', 'cat': 'c', 'ct': 1, 'real': 1.82812, 'bool': True},
         {'_id': 'row4', 'cat': 'c', 'ct': 1, 'real': 0.81271, 'bool': True},
         {'_id': 'row5', 'cat': 'd', 'ct': 2, 'real': 1.14561, 'bool': False},
         {'_id': 'row6', 'cat': 'a', 'ct': 5, 'real': 1.03412, 'bool': False}
        ])
        self.schema2 = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        self.a2 = self.t2.create_analysis(self.schema2, analysis_id="a2",
            force=True)

    def teardown(self):
        self.t.delete()
        self.t2.delete()

    @attr('async')
    def test_make_prediction(self):
        self.a2.wait()
        self.a2.predict({'cat': 'b', 'ct': 2, 'real': None, 'bool': False})

    @attr('async')
    def test_make_prediction_with_empty_row(self):
        self.a2.wait()
        self.a2.predict({})

    @attr('async')
    def test_make_prediction_with_list_of_rows_fails(self):
        self.a2.wait()
        assert_raises(VeritableError, self.a2.predict,
            [{'cat': 'b', 'ct': 2, 'real': None, 'bool': False},
             {'cat': 'b', 'ct': 2, 'real': None, 'bool': None}])

    @attr('async')
    def test_make_prediction_with_count_too_high_fails(self):
        self.a2.wait()
        assert_raises(VeritableError, self.a2.predict,
            {'cat': 'b', 'ct': 2, 'real': None, 'bool': False},
            count=10000)

    @attr('async')
    def test_make_prediction_with_invalid_column_fails(self):
        self.a1.wait()
        assert_raises(VeritableError, self.a1.predict,
            {'cat': 'b', 'ct': 2, 'real': None, 'bool': False})

    @attr('sync')
    def test_delete_analysis_with_instance_method(self):
        self.a2.delete()

    def test_predict_link_is_present(self):
        self.a1.wait()
        self.a1._link('predict')

    @unittest.skip('bug filed')
    def test_predict_from_failed_analysis(self):
        a3 = self.t2.create_analysis(self.schema1, analysis_id="a3",
            force=True)
        wait_for_analysis(a3)
        assert_raises(ServerException, a3.predict,
            {'cat': 'b', 'ct': 2, 'real': None, 'bool': False})
        assert_raises(ServerException, a3.predict, {'zim': None})
        assert_raises(ServerException, a3.predict, {'wos': None})
