import requests
import simplejson as json
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from urlparse import urljoin

class APIKeyException(Exception):
    def __init__(self):
        self.value = """Must provide an API key to instantiate a Veritable connection"""
    def __str__(self):
        return repr(self.value)

class APIBaseURLException(Exception):
    def __init__(self):
        self.value = """Must provide an base URL to instantiate a Veritable connection"""
    def __str__(self):
        return repr(self.value)


def get_response_data(r):
    if r.status_code == requests.codes.ok:
        return json.loads(r.content)
    else:
        handle_http_error(r)

def handle_http_error(r):
    raise r.raise_for_status()

def mgzip(buf):
    wbuf = BytesIO()
    zbuf = GzipFile(
            mode='wb',
            compresslevel=5,
            fileobj=wbuf
            )
    zbuf.write(buf)
    zbuf.close()
    result = wbuf.getvalue()
    wbuf.close()
    return result

def murljoin(base,path):
    return urljoin(base,path.lstrip("/"))

class Connection:
    def __init__(self, api_key, api_base_url):
        if api_key is None:
            raise APIKeyException()
        if api_base_url is None:
            raise APIBaseURLException()        
        self.api_key = api_key
        self.api_base_url = api_base_url.rstrip("/")+"/"
        self.auth = HTTPBasicAuth(self.api_key, self.api_key)
        
    def get(self, url):
        headers = {'accept-encoding': 'gzip'}
        r = requests.get(murljoin(self.api_base_url,url), auth=self.auth, headers = headers)
        return get_response_data(r)
    
    def post(self, url, data):
        headers = {'content-type': 'application/json', 'content-encoding': 'gzip'}
        r = requests.post(murljoin(self.api_base_url,url), data=mgzip(json.dumps(data)), auth=self.auth,
                             headers = headers)
        return get_response_data(r)
    
    def put(self, url, data):
        headers = {'content-type': 'application/json', 'content-encoding': 'gzip'}
        r = requests.put(murljoin(self.api_base_url,url), data=mgzip(json.dumps(data)), auth = self.auth,
                            headers = headers)
        return get_response_data(r)
        
    def delete(self, url):
        r = requests.delete(murljoin(self.api_base_url,url), auth=self.auth)
        return get_response_data(r)
