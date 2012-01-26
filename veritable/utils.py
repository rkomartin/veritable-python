import uuid
from urlparse import urlparse

def make_table_id():
    return uuid.uuid4().hex

def make_analysis_id():
    return uuid.uuid4().hex

def make_row_id():
    return uuid.uuid4().hex

def format_url(*args):
    return "/".join([x.rstrip("/").lstrip("/") for x in args])

def url_has_scheme(url):
	return urlparse(url)[0] is not ""
