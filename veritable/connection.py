import requests
import simplejson as json
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from urlparse import urljoin
from .exceptions import *
from .utils import format_url, url_has_scheme

def debug_log(x):
    print(x)

def fully_qualify_url(f):
    def g(*args, **kwargs):
        url = args[1]
        if not url_has_scheme(url):
            url = format_url(args[0].api_base_url, url)
        return f(args[0], url, *args[2:], **kwargs)
    return g

def get_response_data(r, debug=False):
    if r.status_code == requests.codes.ok:
        return json.loads(r.content)
    else:
        handle_http_error(r, debug)

def handle_http_error(r, debug=False):
    try:
        content = json.loads(r.content)
        if debug:
            debug_log(content)
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
    def __init__(self, api_key, api_base_url, ssl_verify=None,
                 disable_gzip=False, debug=False):
        if api_key is None:
            raise APIKeyException()
        if api_base_url is None:
            raise APIBaseURLException()        
        self.api_key = api_key
        self.api_base_url = format_url(api_base_url)
        self.auth = HTTPBasicAuth(self.api_key, self.api_key)
        self.ssl_verify = ssl_verify
        self.disable_gzip = disable_gzip
        self.debug = debug
        
    @fully_qualify_url
    def get(self, url):
        kwargs = {'headers': {'accept-encoding': 'gzip'},
                  'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        r = requests.get(url, **kwargs)
        return get_response_data(r, self.debug)
    
    @fully_qualify_url
    def post(self, url, data):
        kwargs = {'headers': {'content-type': 'application/json',
                              'content-encoding': 'gzip'},
                  'auth': self.auth}
        if self.disable_gzip:
            kwargs['data'] = json.dumps(data)
        else:
            kwargs['data'] = mgzip(json.dumps(data))
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        r = requests.post(url, **kwargs)
        return get_response_data(r, self.debug)
    
    @fully_qualify_url
    def put(self, url, data):
        kwargs = {'headers': {'content-type': 'application/json',
                              'content-encoding': 'gzip'},
                  'auth': self.auth}
        if self.disable_gzip:
            kwargs['data'] = json.dumps(data)
        else:
            kwargs['data'] = mgzip(json.dumps(data))
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        r = requests.put(url, **kwargs)
        return get_response_data(r, self.debug)
    
    @fully_qualify_url
    def delete(self, url):
        kwargs = {'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
            r = requests.delete(url, **kwargs)
        return get_response_data(r, self.debug)
