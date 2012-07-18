#! usr/bin/python
# coding=utf-8

# NOTE: for py.26 compatibility, comment tests out to skip them -- don't use
# unittest.skip

import veritable
import random
import os
import json
from nose.plugins.attrib import attr
from nose.tools import *
from nose.tools import assert_raises, assert_true, assert_equal
from veritable.exceptions import VeritableError
from veritable.api import Prediction

TEST_API_KEY = os.getenv("VERITABLE_KEY")
TEST_BASE_URL = os.getenv("VERITABLE_URL") or "https://api.priorknowledge.com"
OPTIONS = os.getenv("VERITABLE_NOSETEST_OPTIONS", [])
connect_kwargs = {}
if 'nogzip' in OPTIONS:
    connect_kwargs.update({'enable_gzip': False})
if 'nossl' in OPTIONS:
    connect_kwargs.update({'ssl_verify': False})

INVALID_IDS = ["éléphant", "374.34", "ajfh/d/sfd@#$",
    "\xe3\x81\xb2\xe3\x81\x9f\xe3\x81\xa1\xe3\x81\xae", "", " foo",
    "foo ", " foo ", "foo\n", "foo\nbar", 3, 1.414, False, True,
    "_underscore"]


class TestRowUploads:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)

    def setup(self):
        self.t = self.API.create_table()

    def teardown(self):
        self.t.delete()

    @attr('sync')
    def test_table_upload_row_with_id(self):
        self.t.upload_row({'_id': 'onebug', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_id_json_roundtrip(self):
        id = json.loads(json.dumps(
            {'id': "foo" + str(random.randint(0, 100000000))}))['id']
        self.t.upload_row({'_id': id, 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_upload_row_with_invalid_id(self):
        for id in INVALID_IDS:
            assert_raises(VeritableError, self.t.upload_row,
                {'_id': id, 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_int_id_as_string(self):
        self.t.upload_row({'_id': '3', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_float_id_as_string(self):
        assert_raises(VeritableError, self.t.upload_row,
            {'_id': '3.131455', 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_int_id(self):
        assert_raises(VeritableError, self.t.upload_row,
            {'_id': 3, 'zim': 'zam', 'wos': 19.2})

    @attr('sync')
    def test_table_upload_row_with_float_id(self):
        assert_raises(VeritableError, self.t.upload_row,
            {'_id': 3.131455, 'zim': 'zam', 'wos': 19.2})

    # This should fail per 401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    def test_table_upload_row_with_autogen_id(self):
        assert_raises(VeritableError, self.t.upload_row,
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
        id = json.loads(json.dumps(
            {'id': "sevenbug"}))['id']
        rr = [{'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
             {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
             {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9},
             {'_id': id, 'zim': 'zop', 'wos': 14.9}]
        def wrr():
            for r in rr:
                yield r
        self.t.batch_upload_rows(wrr())

    @attr('sync')
    def test_batch_upload_rows_with_invalid_ids(self):
        rows = []
        for id in INVALID_IDS:
            rows = rows + [{'_id': id, 'zim': 'zop', 'wos': 10.3}]
        assert_raises(VeritableError, self.t.batch_upload_rows, rows)

    # This should fail per https://app.asana.com/0/401264106780/436898020970
    # Our client does not autogenerate row IDs
    @attr('sync')
    def test_batch_upload_rows_with_missing_ids_fails(self):
        assert_raises(VeritableError, self.t.batch_upload_rows,
            [{'zim': 'zop', 'wos': 10.3}, {'zim': 'zam', 'wos': 9.3},
             {'zim': 'zop', 'wos': 18.9},
             {'_id': 'sixbug', 'zim': 'fop', 'wos': 18.3}])

    @attr('sync')
    def test_batch_upload_rows_multipage(self):
        for nrows in [0, 331, 1000, 1421, 2000]:
            t2 = self.API.create_table()
            rs = []
            for i in range(nrows):
                rs.append({'_id': "r" + str(i), 'zim': 'zop', 'wos': random.random(),
                    'fop': random.randint(0,1000)})
            self.t.batch_upload_rows(rs)
            t2.batch_upload_rows(rs)
            rowiter = t2.get_rows()
            assert(len(list(self.t.get_rows())) == nrows)
            self.t.batch_delete_rows([{'_id': str(row['_id'])} for row in rs])
            self.t.batch_upload_rows(t2.get_rows())
            assert(len(list(self.t.get_rows())) == nrows)
            self.t.batch_delete_rows([{'_id': str(row['_id'])} for row in rs])
            t2.delete()

    # FIXME check roundtrips to make sure ids remain valid

    @attr('sync')
    def test_batch_upload_rows_multipage_raise_exception(self):
        rs = []
        for i in range(10000):
            rs.append({'_id': str(i), 'zim': 'zop', 'wos': random.random(),
                'fop': random.randint(0,1000)})
        for pp in [0, -5, 2.31, "foo", False]:
            assert_raises(VeritableError, self.t.batch_upload_rows,
                rs, per_page=pp)


class TestTableOps:
    @classmethod
    def setup_class(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)

    def setup(self):
        self.t = self.API.create_table()
        self.t.batch_upload_rows([
            {'_id': 'onebug', 'zim': 'zam', 'wos': 19.2},
            {'_id': 'twobug', 'zim': 'vim', 'wos': 11.3},
            {'_id': 'threebug', 'zim': 'fop', 'wos': 17.5},
            {'_id': 'fourbug', 'zim': 'zop', 'wos': 10.3},
            {'_id': 'fivebug', 'zim': 'zam', 'wos': 9.3},
            {'_id': 'sixbug', 'zim': 'zop', 'wos': 18.9}])
        self.t2 = self.API.create_table()
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
        assert(len([r for r in self.t.get_rows()]) == 6)

    @attr('sync')
    def test_batch_get_rows_start(self):
        assert(len([r for r in self.t.get_rows(start='onebug')]) == 4)

    @attr('sync')
    def test_batch_get_rows_limit_0(self):
        assert(len([r for r in self.t.get_rows(start='onebug',
            limit=0)]) == 0)

    @attr('sync')
    def test_batch_get_rows_limit_3(self):
        assert(len([r for r in self.t.get_rows(start='onebug',
            limit=3)]) == 3)

    @attr('sync')
    def test_batch_get_rows_limit_higher_than_numrows(self):
        assert(len([r for r in self.t.get_rows(start='onebug',
            limit=100)]) == 4)

    @attr('sync')
    def test_batch_get_rows_start_within_rows(self):
        assert(len([r for r in self.t2.get_rows(start='row2')]) == 5)

    @attr('sync')
    def test_batch_get_rows_start_before(self):
        assert(len([r for r in self.t2.get_rows(start='row0')]) == 6)

    @attr('sync')
    def test_batch_get_rows_start_after(self):
        assert(len([r for r in self.t2.get_rows(start='row7')]) == 0)

# FIXME add separate tests for Cursor class (exercise combos of limit
#    and per_page)
# FIXME add row operations on deleted tables tests
# FIXME make predictions from running analysis
# FIXME make predictions from failed analysis

    @attr('sync')
    def test_delete_row(self):
        self.t.delete_row("fivebug")

    # This is expected behavior according to the API spec
    @attr('sync')
    def test_delete_deleted_row(self):
        self.t.delete_row("fivebug")
        self.t.delete_row("fivebug")

    @attr('sync')
    def test_batch_delete_rows(self):
        rs = list(self.t.get_rows())
        def wrr():
            for r in rs:
                yield r
        self.t.batch_delete_rows(wrr())

    @attr('sync')
    def test_batch_delete_rows_by_id_only(self):
        rs = self.t.get_rows()
        self.t.batch_delete_rows([{'_id': r["_id"]} for r in rs])

    @attr('sync')
    def test_batch_delete_rows_with_some_deleted(self):
        rs = [{'_id': r["_id"]} for r in self.t.get_rows()]
        rs.append({'_id': 'spurious'})
        self.t.batch_delete_rows(rs)

    @attr('sync')
    def test_batch_delete_rows_faulty(self):
        rs = [{'zim': 'zam', 'wos': 9.3},
              {'zim': 'zop', 'wos': 18.9}]
        rs.append(self.t.get_rows())
        assert_raises(VeritableError, self.t.batch_delete_rows, rs)

    @attr('sync')
    def test_get_analyses(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_1", force=True)

        analyses = list(self.t.get_analyses())
        assert_equal(len(analyses), 1)
        for a in analyses:
            assert_true(isinstance(a, veritable.api.Analysis))

    @attr('sync')
    def test_create_analysis_1(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_1", force=True)

    @attr('sync')
    def test_print_analysis(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        a = self.t.create_analysis(schema, analysis_id="zubble_1", force=True)
        print(a)

    @attr('sync')
    def test_create_analysis_2(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, description="Foolish",
            analysis_id="zubble_2", force=True)

    @attr('sync')
    def test_create_analysis_with_id(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, description="Foolish",
            analysis_id="zubble_2", force=True)

    @attr('sync')
    def test_create_table_with_id_json_roundtrip(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        id = json.loads(json.dumps(
            {'id': "zubble2"}))['id']
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
            assert_raises(VeritableError, self.t.create_analysis,
                schema, analysis_id=id)

    @attr('sync')
    def test_create_duplicate_analysis(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        self.t.create_analysis(schema, analysis_id="zubble_2", force=True)
        assert_raises(VeritableError, self.t.create_analysis,
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
        assert_raises(VeritableError, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_2(self):
        schema = 'wimmel'
        assert_raises(VeritableError, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_3(self):
        schema = {}
        assert_raises(VeritableError, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_4(self):
        schema = {'zim': 'real', 'wos': 'real'}
        assert_raises(VeritableError, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_5(self):
        schema = ['categorical', 'real']
        assert_raises(VeritableError, self.t.create_analysis, schema)

    @attr('sync')
    def test_create_analysis_malformed_schema_6(self):
        schema = {'zim': {'type': 'categorical'},
                  'wos': {'type': 'real'},
                  'krob': {'type': 'count'}}
        a = self.t.create_analysis(schema)
        a.wait()
        assert a.state == "failed"

    # Unpossible analysis types are identified synchronously
    @attr('sync')
    def test_create_analysis_faulty_type(self):
        schema = {'zim': {'type': 'categorical'}, 'wos': {'type': 'real'}}
        assert_raises(VeritableError, self.t.create_analysis,
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
        assert isinstance(a.created_at, str)
        assert isinstance(a.finished_at, str)

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
        a = self.t2.create_analysis(schema, analysis_id="test_analysis_1",
            force=True)
        a.wait()
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
        assert_raises(VeritableError, self.t2.get_analysis, "yummy_tummy")

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
    def test_delete_deleted_analysis(self):
        self.t2.delete_analysis("foobar")

    @attr('sync')
    def test_get_analysis_schema(self):
        schema = {'cat': {'type': 'categorical'},
                  'ct': {'type': 'count'},
                  'real': {'type': 'real'},
                  'bool': {'type': 'boolean'}
                  }
        a = self.t2.create_analysis(schema, analysis_id="a", force=True)
        assert schema == a.get_schema()

