import uuid

def make_table_id():
    return uuid.uuid4()

def make_row_id():
    return uuid.uuid4()

def join_url(*args):
    return "/".join(args)
