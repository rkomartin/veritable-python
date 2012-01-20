from requests.auth import HTTPBasicAuth
import requests
import simplejson as json

class APIKeyException(Exception):
    def __init__(self):
        self.value = """"Must provide an API key to instantiate
                         a Veritable connection"""
    def __str__(self):
        return repr(self.value)

def http_req(f):
    def g(*args, **kwargs):
        r = f(*args, **kwargs)
        if r.status_code == requests.codes.ok:
            return json.loads(r.content)
        else:
            handle_http_error(r)
    return g

def handle_http_error(r):
    if r.status_code != requests.codes.bad_request:
        r.raise_for_status()
    try:
        content = json.loads(r.content)
        message = content["message"]
        code = content["code"]
        raise Exception(""""HTTP Error {0} Bad Request {1}:
            \n{2}""".format(r.status_code, code, message))
    except:
        r.raise_for_status()

class Connection:
    def __init__(self, api_key, api_base_url):
        if api_key is None:
            raise APIKeyException()
        self.api_key = api_key
        self.auth = HTTPBasicAuth(self.api_key, None)
        self.BASE_URL = api_base_url
        
    @http_req
    def get(self, url = None):
        if url is None:
            url = self.BASE_URL
        return requests.get(url, auth=self.auth)
    
    @http_req    
    def post(self, url, data):
        headers = {'content-type': 'application/json'}
        return requests.post(url, data=json.dumps(data), auth=self.auth,
                             headers = headers)
    
    @http_req
    def put(self, url, data):
        headers = {'content-type': 'application/json'}
        return requests.put(url, data=json.dumps(data), auth = self.auth,
                            headers = headers)
        
    @http_req    
    def delete(self, url):
        return requests.delete(url, auth=self.auth)
