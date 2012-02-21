import time
import uuid
from math import floor
from random import shuffle
from urlparse import urlparse

def _make_table_id():
    return uuid.uuid4().hex

def _make_analysis_id():
    return uuid.uuid4().hex

def _format_url(*args):
    return "/".join([x.rstrip("/").lstrip("/") for x in args])

def _url_has_scheme(url):
	return urlparse(url)[0] is not ""

def wait_for_analysis(a, poll=2):
	while a.status() == "running":
		time.sleep(poll)

def split_rows(rows, frac):
	N = len(rows)
	inds = range(N)
	shuffle(inds)
	border_ind = int(floor(N * frac))
	train_dataset = [rows[i] for i in inds[0:border_ind]]
	test_dataset = [rows[i] for i in inds[border_ind:]]
	return train_dataset, test_dataset

def _validate_schema(schema):
    for k in schema.keys():
        if not isinstance(k, basestring):
            raise InvalidSchemaException()
    for v in schema.values():
        if not v.keys() == ['type']:
            raise InvalidSchemaException()
        if not len(v.values()) == 1:
            raise InvalidSchemaException()
        if not v.values()[0] in ['boolean', 'categorical', 'real', 'count']:
            raise InvalidSchemaException()

def validate_schema(schema):
	try:
		_validate_schema(schema)
	except:
		return False
	else:
		return True
