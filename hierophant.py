from requests.auth import HTTPBasicAuth
import requests
import simplejson as json
import uuid

APIKeyException = Exception(""""Must provide an API key to instantiate
		a Veritable connection""")

def veritable_connect(api_key):
	return VeritableAPI(VeritableConnection(api_key))

def http_req(f):
	def g(*args):
		r = f(*args)
		if r.status_code == requests.codes.ok:
			return json.loads(r.content)
		else:
			handle_http_error(r)
	return g

def handle_http_error(r):
	raise r.raise_for_status()

def make_table_id():
	return uuid.uuid4()
	
class VeritableConnection:
	BASE_URL = "https://api.priorknowledge.com/tables"
	
	def __init__(self, api_key):
		if api_key is None:
			raise APIKeyException
		self.api_key = api_key
		self.auth = HTTPBasicAuth(self.api_key)
		
	@http_req
	def get(self, url = BASE_URL):
		return requests.get(url, auth=self.auth)
	
	@http_req	
	def post(self, url):
		return requests.post(url, auth=self.auth)
	
	@http_req	
	def delete(self, url):
		return requests.delete(url, auth=self.auth)
			
class VeritableAPI:
	def __init__(self, connection):
		self.api_key = api_key
		self.connection = connection
		
	def __repr__(self):
		return "<<Hierophant %s>>" % id(self)
	
	def entry(self):
		"""Return the entry point into the API"""
		return self.connection.get()
		
	def tables(self):
		pass
		

class VeritableTable:
	def __init__(self, table_id):
		self.table_id = table_id
		
	def get():
		pass
		
	def delete():
		pass
		
	def add_rows():
		pass
		
	def get_row():
		pass
		
	def list_analyses():
		pass
		
class VeritableAnalysis:
	def get():
		pass
		
	def learn():
		pass
		
	def delete():
		pass
		
	def predict():
		pass
	