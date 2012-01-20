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
    raise r.raise_for_status()

class Connection:
    BASE_URL = "https://api.priorknowledge.com/tables"
    
    def __init__(self, api_key):
        if api_key is None:
            raise APIKeyException()
        self.api_key = api_key
        self.auth = HTTPBasicAuth(self.api_key)
        
    @http_req
    def get(self, url = BASE_URL):
        return requests.get(url, auth=self.auth, headers = headers)
    
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
