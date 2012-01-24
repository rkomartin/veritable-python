import os
import json
from API import *
from connection import *
from nose.tools import raises
from requests.auth import HTTPBasicAuth


class MockConnection:
	def __init__(self, api_key, api_base_url, ssl_verify = True):
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
		return (url, self.ssl_verify, self.auth, headers)
	
	@fully_qualify_url
	def post(self, url, data):
		headers = {'content-type': 'application/json',
					 'content-encoding': 'gzip'}
		return (url, self.ssl_verify, json.dumps(data), self.auth, headers)

	@fully_qualify_url
	def put(self, url, data):
		headers = {'content-type': 'application/json',
					 'content-encoding': 'gzip'}
		return (url, self.ssl_verify, json.dumps(data), self.auth, headers)
	
	@fully_qualify_url
	def delete(self, url):
		return (url, self.ssl_verify, json.dumps(data), self.auth)

api_key = "jellokey"
api_base_url = "https://www.priorknowledge.com/api"

mconn = MockConnection(api_key, api_base_url)
mAPI = API(mconn)

def test_veritable_connect():
	c = veritable_connect(api_key, api_base_url)
	assert isinstance(c, API) and c.api_key == api_key
	  && c.api_base_url == api_base_url

def test_API_creation():
	assert mAPI.connection == mconn

def test_table_parsing():
	tt = parse_tables()
# veritable_connect returns an API, with and without manually setting the base_url; with ssl_verify = True, False, path to cert