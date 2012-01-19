from requests.auth import HTTPBasicAuth
import requests
import simplejson as json
import uuid

class APIKeyException(Exception):
	def __init__(self):
		self.value = """"Must provide an API key to instantiate
						 a Veritable connection"""
	def __str__(self):
		return repr(self.value)

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

def make_row_id():
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
		return [VeritableTable(self.connection, t) for t in r["data"]]
	
	def create_table(self, table_id = make_table_id(), description = ""):
		"""Create a table with the given id."""	
		r = self.connection.put(self.connection.BASE_URL + "/" + table_id,
								data = {"description": description})
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

	def get(self):
		data = self.connection.get(self.links["self"])
		if "description" in data:
			self.description = data["description"]
		self.id = data["_id"]
		self.last_updated = data["last_updated"]
		self.links = {"self": data["links"]["self"],
					  "analyses": data["links"]["analyses"],
					  "rows": data["links"]["rows"]}	
		return self
		
	def delete(self):
		return self.connection.delete(self.links["self"])
		
	def add_row(self, row):
		if "_id" in row:
			row_id = row["_id"]
		else:
			row_id = make_row_id()
		return self.connection.put(self.links["rows"] + "/" + row_id, row)
		
	def add_rows(self, data):
		return self.connection.post(self.links["rows"], data)
		
	def get_row(self, row_id):
		return self.connection.get(self.links["rows"] + "/" + row_id)

	def get_rows(self):
		return self.connection.get(self.links["rows"])
		
	def delete_row(self, row_id):
		return self.connection.delete(self.links["rows"] + "/" + row_id)
		
	def get_analyses(self):
		r = self.connection.get(self.links["analyses"])
		return [VeritableAnalysis(self.connection, a) for a in r["data"]]
	
	def create_analysis(self, schema, description = "",
						analysis_id = make_analysis_id(), type = "veritable"):
		r = self.connection.put(self.links["analyses"] + "/" + analysis_id,
								data = {"description": description,
										"type": type,
										"schema": schema})
class VeritableAnalysis:
	def __init__(self, connection, data):
		self.connection = connection
		self.last_learned = data["last_learned"]
		if "last_updated" in data
			self.last_updated = data["last_updated"]
		self.links = {"self": data["links"]["self"],
					  "schema": data["links"]["schema"],
					  "learn": data["links"]["learn"],
					  "predict": data["links"]["predict"]}
	def get(self):
		data = self.connection.get(self.links["self"])
		self.last_learned = data["last_learned"]
		if "last_updated" in data
			self.last_updated = data["last_updated"]
		self.links = {"self": data["links"]["self"],
					  "schema": data["links"]["schema"],
					  "learn": data["links"]["learn"],
					  "predict": data["links"]["predict"]}
		return self
		
	def learn(self):
		return self.connection.post(self.links["learn"])
		
	def delete(self):
		return self.connection.delete(self.links["self"])

	def get_schema(self):
		return self.connection.get(self.links["schema"])
		
	def predict(self, request):
		return self.connection.post(self.links["predict"], data = request)
		