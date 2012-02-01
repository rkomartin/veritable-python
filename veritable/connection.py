import requests
import simplejson as json
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from urlparse import urljoin
from veritable.exceptions import *
from veritable.utils import format_url, url_has_scheme

def fully_qualify_url(f):
    def g(*args, **kwargs):
        url = args[1]
        if not url_has_scheme(url):
            url = format_url(args[0].api_base_url, url)
        return f(args[0], url, *args[2:], **kwargs)
    return g

def get_response_data(r):
    if r.status_code == requests.codes.ok:
        return json.loads(r.content)
    else:
        handle_http_error(r)

def handle_http_error(r):
    try:
        content = json.loads(r.content)
        message = content["message"]
        code = content["code"]
    except:
        r.raise_for_status()
    else:
        if r.status_code == requests.codes.not_found:
            raise ServerException("""HTTP Error {0} Not Found -- {1}:
                \n{2}""".format(r.status_code, code, message))
        if r.status_code == requests.codes.bad_request:
            raise ServerException("""HTTP Error {0} Bad Request -- {1}:
                \n{2}""".format(r.status_code, code, message))
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
        self.ssl_verify = ssl_verify
        
    @fully_qualify_url
    def get(self, url):
        headers = {'accept-encoding': 'gzip'}
        r = requests.get(url, verify=self.ssl_verify, auth=self.auth,
                           headers=headers)
        return get_response_data(r)
    
    @fully_qualify_url
    def post(self, url, data):
        headers = {'content-type': 'application/json',
                     'content-encoding': 'gzip'}
        r = requests.post(url, verify=self.ssl_verify,
                            data=mgzip(json.dumps(data)),
                            auth=self.auth, headers=headers)
        return get_response_data(r)
    
    @fully_qualify_url
    def put(self, url, data):
        headers = {'content-type': 'application/json',
                     'content-encoding': 'gzip'}
        r = requests.put(url,verify=self.ssl_verify,
                           data=mgzip(json.dumps(data)),
                           auth=self.auth, headers=headers)
        return get_response_data(r)
    
    @fully_qualify_url
    def delete(self, url):
        r = requests.delete(url, verify=self.ssl_verify, auth=self.auth)
        return get_response_data(r)
