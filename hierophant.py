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
	def put(self, url, data):
		return requests.put(url, data, auth = self.auth)
		
	@http_req	
	def delete(self, url):
		return requests.delete(url, auth=self.auth)
			
class VeritableAPI:
	def __init__(self, connection):
		self.connection = connection
			
	def tables(self):
		"""Return the Veritable tables available to the user."""
		r = self.connection.get()
		ts = [VeritableTable(self.connection, t) for t in r["data"]]
		return ts
	
	def create_table(self, table_id = make_table_id(), description):
		"""Create a table with the given id."""	
		if description is not None:
			r = self.connection.put(self.connection.BASE_URL + "/" + table_id,
									data = {"description": description})
		else:
			r = self.connection.put(self.connection.BASE_URL + "/" + table_id)
		return VeritableTable(self.connection, r)

class VeritableTable:
	def __init__(self, connection, data):
		self.connection = connection
		if "description" in data:
			self.description = data["description"]
		self.id = data["_id"]
		self.last_updated = data["last_updated"]
		self.links = {"self": data["links"]["self"],
					  "analyses": data["links"]["analyses"],
					  "rows": data["links"]["rows"]}		

	def get():
		data = self.connection.get(self.links["self"])
		if "description" in data:
			self.description = data["description"]
		self.id = data["_id"]
		self.last_updated = data["last_updated"]
		self.links = {"self": data["links"]["self"],
					  "analyses": data["links"]["analyses"],
					  "rows": data["links"]["rows"]}	
		return self
		
	def delete():
		r = self.connection.delete(self.links["self"])
		return r
		
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
	