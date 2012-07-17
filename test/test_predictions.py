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


class TestPredictions:
    @classmethod
    def setupClass(self):
        self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL,
            **connect_kwargs)
        self.t2 = self.API.create_table()
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
        self.a2.wait()
        self.a3 = self.t2.create_analysis(self.schema2, analysis_id="a3",
            force=True)

    @classmethod
    def teardownClass(self):
        self.t2.delete()

    def _check_preds(self, schema_ref, reqs, preds):
        preds = list(preds)
        assert len(reqs)==len(preds)
        for i in range(len(reqs)):
            req = reqs[i]
            pr = preds[i]
            assert(isinstance(pr, dict))
            assert(isinstance(pr, veritable.api.Prediction))
            assert(isinstance(pr.uncertainty, dict))
            if '_request_id' in req:
                assert req['_request_id'] == pr.request_id
                assert len(req) == (len(pr)+1)
            else:
                assert len(req) == len(pr)
                assert pr.request_id is None                
            for k in pr:
                try:
                    if isinstance(schema_ref[k], basestring):
                        assert(isinstance(pr[k], basestring))
                    else:
                        assert(isinstance(pr[k], type(schema_ref[k])))
                except:
                    assert(isinstance(pr[k], type(schema_ref[k])))
                assert (pr[k] == req[k] or req[k] is None)
                assert(isinstance(pr.uncertainty[k], float))
        assert(isinstance(pr.distribution, list))
        for d in pr.distribution:
            assert(isinstance(d, dict))
            if '_request_id' in req:
                assert len(req) == (len(d)+1)
            else:
                assert len(req) == len(d)
            for k in d:
                try:
                    if isinstance(schema_ref[k], basestring):
                        assert(isinstance(d[k], basestring))
                    else:
                        assert(isinstance(d[k], type(schema_ref[k])))
                except:
                    assert(isinstance(d[k], type(schema_ref[k])))
                assert (d[k] == req[k] or req[k] is None)
   
    @attr('async')
    def test_make_prediction(self):
        schema_ref = json.loads(json.dumps({'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}))
        r = json.loads(json.dumps({'cat': 'b', 'ct': 2, 'real': None, 'bool': False}))
        pr = self.a2.predict(r)
        self._check_preds(schema_ref,[r],[pr])
        r = json.loads(json.dumps({'_request_id': 'foo', 'cat': 'b', 'ct': 2,
            'real': None, 'bool': False}))
        pr = self.a2.predict(r)
        self._check_preds(schema_ref,[r],[pr])

    @attr('async')
    def test_make_batch_prediction(self):
        schema_ref = json.loads(json.dumps({'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}))
        rr = [json.loads(json.dumps(
            {'_request_id': str(i), 'cat': 'b', 'ct': 2, 'real': None,
            'bool': False})) for i in range(1)]
        prs = self.a2.batch_predict(rr)
        self._check_preds(schema_ref,rr,prs)
        rr = [json.loads(json.dumps(
            {'_request_id': str(i), 'cat': 'b', 'ct': 2, 'real': None,
            'bool': False})) for i in range(10)]
        prs = self.a2.batch_predict(rr)
        self._check_preds(schema_ref,rr,prs)
                
    @attr('async')
    def test_make_prediction_with_empty_row(self):
        self.a2.predict({})

    @attr('async')
    def test_make_prediction_with_invalid_column_fails(self):
        assert_raises(VeritableError, self.a2.predict,
            {'cat': 'b', 'ct': 2, 'real': None, 'jello': False})

    @attr('async')
    def test_make_batch_prediction_missing_request_id_fails(self):
        assert_raises(VeritableError, list, self.a2.batch_predict(
            [{'cat': 'b', 'ct': 2, 'real': None, 'bool': False},
             {'cat': 'b', 'ct': 2, 'real': None, 'bool': None}]))

    @attr('async')
    def test_batch_prediction_batching(self):
        schema_ref = {'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}
        rr = [ {'_request_id':'a', 'cat': None, 'ct': 2,
                  'real': 4.0, 'bool': False},
                 {'_request_id':'b','cat': 'b', 'ct': 2,
                  'real': None, 'bool': True},
                 {'_request_id':'c','cat': 'b', 'ct': None,
                  'real': 3.8, 'bool': True} ]
        prs = self.a2._predict(rr, count=10, maxcells=30, maxcols=4)
        self._check_preds(schema_ref,rr,prs)
        prs = self.a2._predict(rr, count=10, maxcells=20, maxcols=4)
        self._check_preds(schema_ref,rr,prs)
        prs = self.a2._predict(rr, count=10, maxcells=17, maxcols=4)
        self._check_preds(schema_ref,rr,prs)
        prs = self.a2._predict(rr, count=10, maxcells=10, maxcols=4)
        self._check_preds(schema_ref,rr,prs)

    @attr('async')
    def test_batch_prediction_count_batching(self):
        schema_ref = {'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}
        rr = [ {'_request_id':'a', 'cat': None, 'ct': 2,
                  'real': 4.0, 'bool': False},
                 {'_request_id':'b','cat': 'b', 'ct': 2,
                  'real': None, 'bool': True},
                 {'_request_id':'c','cat': 'b', 'ct': None,
                  'real': 3.8, 'bool': True} ]
        prs = self.a2._predict(rr, count=10, maxcells=1, maxcols=4)
        self._check_preds(schema_ref,rr,prs)
    
    @attr('async')
    def test_batch_prediction_streaming(self):
        schema_ref = {'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}
        rr = [ {'_request_id':'a', 'cat': None, 'ct': 2,
                  'real': 4.0, 'bool': False},
                 {'_request_id':'b','cat': 'b', 'ct': 2,
                  'real': None, 'bool': True},
                 {'_request_id':'c','cat': 'b', 'ct': None,
                  'real': 3.8, 'bool': True} ]
        def wrr():
            for r in rr:
                yield r
        prs = self.a2._predict(wrr(), count=10, maxcells=5)
        self._check_preds(schema_ref,rr,prs)

    @attr('async')
    def test_batch_prediction_too_many_cells(self):
        schema_ref = {'cat': 'b', 'ct': 2, 'real': 3.1, 'bool': False}
        rr = [ {'_request_id':'a', 'cat': None, 'ct': None,
                  'real': 4.0, 'bool': False} ]
        prs = self.a2._predict(rr, count=10, maxcells=20, maxcols=4)
        self._check_preds(schema_ref,rr,prs)
        assert_raises(VeritableError, list, self.a2._predict( rr, count=10, maxcells=20, maxcols=3))
        assert_raises(VeritableError, list, self.a2._predict( rr, count=10, maxcells=1, maxcols=4))

    @attr('async')
    def test_make_predictions_with_fixed_int_val_for_float_col(self):
        self.a2.predict({'cat': None, 'ct': None, 'real': 1, 'bool': None})

    @attr('async')
    def test_delete_analysis_with_instance_method(self):
        self.a3.delete()

    @attr('async')
    def test_predict_link_is_present(self):
        self.a2._link('predict')


class TestPredictionUtils:
    def setup(self):
        request = {'ColInt': None, 'ColFloat': None,
            'ColCat': None, 'ColBool': None}
        schema = {'ColInt': {'type': 'count'}, 'ColFloat': {'type': 'real'},
            'ColCat': {'type': 'categorical'}, 'ColBool': {'type': 'boolean'}}
        distribution = [{'ColInt':3, 'ColFloat':3.1, 'ColCat': 'a', 'ColBool':False},
            {'ColInt':4, 'ColFloat':4.1, 'ColCat': 'b', 'ColBool':False},
            {'ColInt':8, 'ColFloat':8.1, 'ColCat': 'b', 'ColBool':False},
            {'ColInt':11, 'ColFloat':2.1, 'ColCat': 'c', 'ColBool':True}]
        self.testpreds = Prediction(request, distribution, schema)
        self.testpreds2 = Prediction(json.loads(json.dumps(request)),
            json.loads(json.dumps(distribution)), json.loads(json.dumps(schema)))

    def test_summarize_count(self):
        for tp in [self.testpreds, self.testpreds2]:
            expected = tp['ColInt']
            uncertainty = tp.uncertainty['ColInt']
            assert isinstance(expected, int)
            assert expected == int(round((3 + 4 + 8 + 11) / 4.0))
            assert abs(uncertainty - 8) < 0.001
            p_within = tp.prob_within('ColInt',(5,9))
            assert abs(p_within - 0.25) < 0.001
            c_values = tp.credible_values('ColInt')
            assert c_values == (3,11)
            c_values = tp.credible_values('ColInt',p=0.60)
            assert c_values == (4,8)

    def test_summarize_real(self):
        for tp in [self.testpreds, self.testpreds2]:
            expected = tp['ColFloat']
            uncertainty = tp.uncertainty['ColFloat']
            assert isinstance(expected, float)
            assert abs(expected - 4.35) < 0.001
            assert abs(uncertainty - 6) < 0.001
            p_within = tp.prob_within('ColFloat',(5,9))
            assert abs(p_within - 0.25) < 0.001
            c_values = tp.credible_values('ColFloat')
            assert c_values == (2.1,8.1)
            c_values = tp.credible_values('ColFloat',p=0.60)
            assert c_values == (3.1,4.1)

    def test_summarize_cat(self):
        for tp in [self.testpreds, self.testpreds2]:
            expected = tp['ColCat']
            uncertainty = tp.uncertainty['ColCat']
            try:
                isinstance(expected, basestring)
            except:
                assert isinstance(expected, str)
            else:
                assert isinstance(expected, basestring)
            assert expected == 'b'
            assert abs(uncertainty - 0.5) < 0.001
            p_within = tp.prob_within('ColCat',['b','c'])
            assert abs(p_within - 0.75) < 0.001
            c_values = tp.credible_values('ColCat')
            assert c_values == {'b': 0.5}
            c_values = tp.credible_values('ColCat',p=0.10)
            assert c_values == {'a': 0.25, 'b': 0.5, 'c': 0.25}

    def test_summarize_bool(self):
        for tp in [self.testpreds, self.testpreds2]:
            expected = tp['ColBool']
            uncertainty = tp.uncertainty['ColBool']
            assert isinstance(expected, bool)
            assert expected == False
            assert abs(uncertainty - 0.25) < 0.001
            p_within = tp.prob_within('ColBool',[True])
            assert abs(p_within - 0.25) < 0.001
            c_values = tp.credible_values('ColBool')
            assert c_values == {False: 0.75}
            c_values = tp.credible_values('ColBool',p=0.10)
            assert c_values == {True: 0.25, False: 0.75}

