import logging
import requests
import json
import sys
from gzip import GzipFile
from io import BytesIO
from requests.auth import HTTPBasicAuth
from .exceptions import (ServerException, APIKeyException,
    APIBaseURLException)
from .utils import _url_has_scheme
from .version import __version__

USER_AGENT = "veritable-python " + __version__


def _fully_qualify_url(f):
    # ensures that urls passed to the HTTP methods are fully qualified
    def g(*args, **kwargs):
        url = args[1]
        if not _url_has_scheme(url):
            url = args[0].api_base_url.rstrip("/") + "/" + url.lstrip("/")
        return f(args[0], url, *args[2:], **kwargs)
    return g


def _get_response_data(r, debug_log=None):
    # routes HTTP errors, if any, and translates JSON response data.
    if r.status_code == requests.codes.ok:
        if debug_log is not None:
            debug_log(json.loads(r.content.decode('utf-8')))
        return json.loads(r.content.decode('utf-8'))
    else:
        handle_http_error(r, debug_log)


def handle_http_error(r, debug_log=None):
    # handles HTTP errors.
    try:
        content = json.loads(r.content.decode('utf-8'))
        if debug_log is not None:
            debug_log(content)
        message = content["message"]
        code = content["code"]
    except:
        r.raise_for_status()
    else:
        if r.status_code == requests.codes.not_found:
            raise ServerException("""HTTP Error {0} Not Found -- {1}: \
            {2}""".format(r.status_code, code, message))
        if r.status_code == requests.codes.bad_request:
            raise ServerException("""HTTP Error {0} Bad Request -- {1}: \
                {2}""".format(r.status_code, code, message))
        r.raise_for_status()


def _mgzip(buf):
    # gzip middleware.
    wbuf = BytesIO()
    zbuf = GzipFile(
            mode='wb',
            compresslevel=5,
            fileobj=wbuf
            )
    zbuf.write(buf.encode('utf-8'))
    zbuf.close()
    result = wbuf.getvalue()
    wbuf.close()
    return result


class Connection:
    """Wraps the raw HTTP requests to the Veritable server."""
    def __init__(self, api_key, api_base_url, ssl_verify=None,
                 enable_gzip=True, debug=False):
        if api_key is None:
            raise APIKeyException()
        if api_base_url is None:
            raise APIBaseURLException()
        self.api_key = api_key
        self.api_base_url = api_base_url.rstrip("/")
        self.auth = HTTPBasicAuth(self.api_key, "")
        self.ssl_verify = ssl_verify
        self.disable_gzip = not(enable_gzip)
        self.debug = debug
        self.session = self._create_session()
        if self.debug:
            self.logger = logging.getLogger(__name__)
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            self.logger.addHandler(ch)
            self.logger.setLevel(logging.DEBUG)

    def __str__(self):
        return "<veritable.Connection url='" + self._api_base_url + "'>"

    def __repr__(self):
        return self.__str__()

    def _create_session(self):
        # Creates a requests session
        headers = {'User-Agent': USER_AGENT}
        return requests.session(auth=self.auth, headers=headers)

    def debug_log(self, x):
        """Debug logging."""
        if self.debug:
            self.logger.debug(x)

    @_fully_qualify_url
    def get(self, url):
        """Wraps GET requests."""
        kwargs = {'headers': {}, 'prefetch': True}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['headers']['Accept-Encoding'] = 'gzip'
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.get(url, **kwargs)
        return _get_response_data(r, self.debug_log)

    @_fully_qualify_url
    def post(self, url, data):
        """Wraps POST requests."""
        kwargs = {'headers': {'Content-Type': 'application/json'},
                  'prefetch': True}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = _mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.post(url, **kwargs)
        return _get_response_data(r, self.debug_log)

    @_fully_qualify_url
    def put(self, url, data):
        """Wraps PUT requests."""
        kwargs = {'headers': {'Content-Type': 'application/json'},
                  'prefetch': True}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if not self.disable_gzip:
            kwargs['data'] = _mgzip(json.dumps(data))
            kwargs['headers']['Content-Encoding'] = 'gzip'
        else:
            kwargs['data'] = json.dumps(data)
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.put(url, **kwargs)
        return _get_response_data(r, self.debug_log)

    @_fully_qualify_url
    def delete(self, url):
        """Wraps DELETE requests."""
        kwargs = {'headers': {}, 'prefetch': True}
        if self.ssl_verify is not None:
            kwargs['verify'] = self.ssl_verify
        if self.debug:
            kwargs['config'] = {'verbose': sys.stderr}
        r = self.session.delete(url, **kwargs)
        return _get_response_data(r, self.debug_log)
