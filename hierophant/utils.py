import uuid

def make_table_id():
    return uuid.uuid4().hex

def make_analysis_id():
    return uuid.uuid4().hex

def make_row_id():
    return uuid.uuid4().hex

def join_url(*args):
    return "/".join(args)
