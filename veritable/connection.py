import logging
import requests
import simplejson as json
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from urlparse import urljoin
from .exceptions import *
from .utils import _format_url, _url_has_scheme

USER_AGENT = "veritable-python"

def fully_qualify_url(f):
    def g(*args, **kwargs):
        url = args[1]
        if not _url_has_scheme(url):
            url = _format_url(args[0].api_base_url, url)
        return f(args[0], url, *args[2:], **kwargs)
    return g

def get_response_data(r, debug_log=None):
    if r.status_code == requests.codes.ok:
        if debug_log is not None:
            debug_log(json.loads(r.content))
        return json.loads(r.content)
    else:
        handle_http_error(r, debug_log)

def handle_http_error(r, debug_log=None):
    try:
        content = json.loads(r.content)
        if debug_log is not None:
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
        self.api_base_url = _format_url(api_base_url)
        self.auth = HTTPBasicAuth(self.api_key, self.api_key)
        self.ssl_verify = ssl_verify
        self.disable_gzip = disable_gzip
        self.debug = debug
        if self.debug:
            self.logger = logging.getLogger(__name__)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            self.logger.addHandler(ch)
            self.logger.setLevel(logging.DEBUG)

    def debug_log(self, x):
        if self.debug:
            self.logger.debug(x)

        
    @fully_qualify_url
    def get(self, url):
        kwargs = {'headers': {'User-Agent':USER_AGENT},
                  'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['headers']['Accept-Encoding'] = 'gzip'
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = requests.get(url, **kwargs)
        return get_response_data(r, self.debug_log)
    
    @fully_qualify_url
    def post(self, url, data):
        kwargs = {'headers': {'User-Agent':USER_AGENT,'Content-Type': 'application/json'},
                  'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = requests.post(url, **kwargs)
        return get_response_data(r, self.debug_log)
    
    @fully_qualify_url
    def put(self, url, data):
        kwargs = {'headers': {'User-Agent':USER_AGENT,'Content-Type': 'application/json'},
                  'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = requests.put(url, **kwargs)
        return get_response_data(r, self.debug_log)
    
    @fully_qualify_url
    def delete(self, url):
        kwargs = {'headers': {'User-Agent':USER_AGENT},
                  'auth': self.auth}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = requests.delete(url, **kwargs)
        return get_response_data(r, self.debug_log)
