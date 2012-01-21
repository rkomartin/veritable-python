import requests
import simplejson as json
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from urlparse import urljoin
from hierophant.utils import format_url

class APIKeyException(Exception):
    def __init__(self):
        self.value = """Must provide an API key to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)

class APIBaseURLException(Exception):
    def __init__(self):
        self.value = """Must provide an base URL to instantiate a Veritable
                        connection"""
    def __str__(self):
        return repr(self.value)


def get_response_data(r):
    if r.status_code == requests.codes.ok:
        return json.loads(r.content)
    else:
        handle_http_error(r)

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

class Connection:
    def __init__(self, api_key, api_base_url, ssl_verify=True):
        if api_key is None:
            raise APIKeyException()
        if api_base_url is None:
            raise APIBaseURLException()        
        self.api_key = api_key
        self.api_base_url = format_url(api_base_url)
        self.auth = HTTPBasicAuth(self.api_key, self.api_key)
        self.ssl_verify=ssl_verify
        
    def get(self, url):
        headers = {'accept-encoding': 'gzip'}
        r = requests.get(format_url(self.api_base_url, url),
                           verify=self.ssl_verify, auth=self.auth,
                           headers=headers)
        return get_response_data(r)
    
    def post(self, url, data):
        headers = {'content-type': 'application/json',
                     'content-encoding': 'gzip'}
        r = requests.post(format_url(self.api_base_url,url), 
                            verify=self.ssl_verify, 
                            data=mgzip(json.dumps(data)), auth=self.auth,
                            headers=headers)
        return get_response_data(r)
    
    def put(self, url, data):
        headers = {'content-type': 'application/json',
                     'content-encoding': 'gzip'}
        r = requests.put(format_url(self.api_base_url,url), 
                           verify=self.ssl_verify,
                           data=mgzip(json.dumps(data)), auth=self.auth,
                           headers=headers)
        return get_response_data(r)
        
    def delete(self, url):
        r = requests.delete(format_url(self.api_base_url,url),
                              verify=self.ssl_verify, auth=self.auth)
        return get_response_data(r)
