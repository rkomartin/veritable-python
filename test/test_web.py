from gzip import GzipFile
from io import BytesIO
from flask import Flask, request
import os
from veritable.connection import USER_AGENT

class GzipMiddleware(object):
    def __init__(self,
                 app,
                 compresslevel=5
                 ):
        self.app = app
        self.compresslevel = compresslevel

    def __call__(self, environ, start_response):
        # Handle gunzip gzip-encoded requests
        # Reference: http://www.mail-archive.com/dulwich-users@lists.launchpad.net/msg00470.html
        if 'gzip' in environ.get('HTTP_CONTENT_ENCODING', ''):
            print("Unzipping request body")
            zlength = int(environ.get('CONTENT_LENGTH', '0'))
            rbytes = environ['wsgi.input'].read(zlength) if zlength > 0 else environ['wsgi.input'].read()
            environ['wsgi.input'].close()
            rbuf = BytesIO(rbytes)
            zbuf = GzipFile(
                mode='rb',
                fileobj=rbuf
                )
            wbuf = BytesIO()
            wbuf.write(zbuf.read())
            zbuf.close()
            rbuf.close()
            environ.pop('HTTP_CONTENT_ENCODING')
            environ['CONTENT_LENGTH'] = str(wbuf.tell())
            wbuf.seek(0)
            environ['wsgi.input'] = wbuf        

        # Handle gzipping responses if client can accept gzip
        # Reference: http://pylonsbook.com/en/1.1/the-web-server-gateway-interface-wsgi.html
        if 'gzip' not in environ.get('HTTP_ACCEPT_ENCODING', ''):
            return self.app(environ, start_response)
        print("Zipping response body")
        wbuf = BytesIO()
        output = GzipFile(
            mode='wb',
            compresslevel=self.compresslevel,
            fileobj=wbuf
            )
        start_response_args = []
        def dummy_start_response(status, headers, exc_info=None):
            start_response_args.append(status)
            start_response_args.append(headers)
            start_response_args.append(exc_info)
            return output.write
        app_iter = self.app(environ, dummy_start_response)
        for line in app_iter:
            output.write(line)
        if hasattr(app_iter, 'close'):
            app_iter.close()
        output.close()
        wbuf.seek(0)
        result = wbuf.getvalue()
        wbuf.close()
        headers = []
        for name, value in start_response_args[1]:
            if name.lower() != 'content-length':
                 headers.append((name, value))
        headers.append(('Content-Length', str(len(result))))
        headers.append(('Content-Encoding', 'gzip'))
        start_response(start_response_args[0], headers, start_response_args[2])
        return [result]


app = Flask(__name__)
app.wsgi_app = GzipMiddleware(app.wsgi_app)

@app.route("/echopost", methods=['POST'])
def echopost():
    assert request.headers['User-Agent'] == USER_AGENT
    return request.data

@app.route("/echoput", methods=['PUT'])
def echoput():
    assert request.headers['User-Agent'] == USER_AGENT
    return request.data

@app.route("/echoget/<content>", methods=['GET'])
def echoget(content):
    assert request.headers['User-Agent'] == USER_AGENT
    return '"'+content+'"'

@app.route("/echodelete/<content>", methods=['DELETE'])
def echodelete(content):
    assert request.headers['User-Agent'] == USER_AGENT
    return '"'+content+'"'

@app.route("/inst/echoget2/<content>", methods=['GET'])
def echoget(content):
    assert request.headers['User-Agent'] == USER_AGENT
    return '"'+content+'"'

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
