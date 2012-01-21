import uuid

def make_table_id():
    return uuid.uuid4().hex

def make_analysis_id():
    return uuid.uuid4().hex

def make_row_id():
    return uuid.uuid4().hex

def format_url(*args):
    return "/".join([x.rstrip("/").lstrip("/") for x in args])
