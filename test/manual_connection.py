from veritable.connection import (Connection, APIKeyException,
    APIBaseURLException)
import os
from simple_web import app
from multiprocessing import Process
from nose.tools import raises


port = int(os.environ.get("PORT", 5348))
host = "127.0.0.1"
apikey = "jellokey"

web_process = Process(target=lambda x: app.run(host=host, port=x),
    args=(port,))

url_base = "http://" + host + ":" + str(port)
conn = Connection(api_key=apikey, api_base_url=url_base)

url_base2 = "http://" + host + ":" + str(port) + "/inst"
conn2 = Connection(api_key=apikey, api_base_url=url_base2)

url_base2a = "http://" + host + ":" + str(port) + "/inst/"
conn2a = Connection(api_key=apikey, api_base_url=url_base2a)


def setUp():
    web_process.start()


def tearDown():
    web_process.terminate()


def test_get():
    content = 'jello'
    response = conn.get(url_base + '/echoget/' + content)
    assert str(response) == str(content)


def test_delete():
    content = 'jello'
    response = conn.delete(url_base + '/echodelete/' + content)
    assert str(response) == str(content)


def test_post():
    content = {'key': ['val1', 'val2', 3, 4]}
    response = conn.post(url_base + '/echopost', content)
    assert response == content


def test_put():
    content = {'key': ['val1', 'val2', 3, 4]}
    response = conn.put(url_base + '/echoput', content)
    assert response == content


def test_fq_url():
    content = 'jello'
    response = conn2.get(url_base2 + '/echoget2/' + content)
    assert str(response) == str(content)


def test_rbase_abspath():
    content = 'jello'
    response = conn2.get('/echoget2/' + content)
    assert str(response) == str(content)


def test_rbase_rpath():
    content = 'jello'
    response = conn2.get('echoget2/' + content)
    assert str(response) == str(content)


def test_abase_abspath():
    content = 'jello'
    response = conn2a.get('/echoget2/' + content)
    assert str(response) == str(content)


def test_abase_rpath():
    content = 'jello'
    response = conn2a.get('echoget2/' + content)
    assert str(response) == str(content)


@raises(APIKeyException)
def test_noapikey():
    Connection(api_key=None, api_base_url=url_base)


@raises(APIBaseURLException)
def test_nobaseurl():
    Connection(api_key=apikey, api_base_url=None)
