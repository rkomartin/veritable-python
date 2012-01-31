from veritable.api import *
import simplejson as json
import time

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"
DATA_FILE = "mammals.json"

data = json.load(open(DATA_FILE, 'r'))


def wait_for_analysis(analysis, poll=10):
	while analysis.status() == "pending":
		time.sleep(poll)
	if analysis.status() == "failed":
		raise Exception(analysis.get_state()["error"])

class TestMammals:
	def setup():
		self.API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)
		self.mammals = self.API.create_table("mammals", force=True)
		self.mammals.add_rows(data)
		self.schema = {
			'furry': {'type': 'boolean'},
			'hairless': {'type': 'boolean'},
			'big': {'type': 'boolean'},
			'small': {'type': 'boolean'},
			'flippers': {'type': 'boolean'},
			'hands': {'type': 'boolean'},
			'hooves': {'type': 'boolean'},
			'paws': {'type': 'boolean'},
			'longneck': {'type': 'boolean'},
			'tail': {'type': 'boolean'},
			'filter_feeder': {'type': 'boolean'},
			'horns': {'type': 'boolean'},
			'claws': {'type': 'boolean'},
			'tusks': {'type': 'boolean'},
			'flys': {'type': 'boolean'},
			'tunnels': {'type': 'boolean'},
			'walks': {'type': 'boolean'},
			'nocturnal': {'type': 'boolean'},
			'eats_fish': {'type': 'boolean'},
			'eats_meat': {'type': 'boolean'},
			'eats_plankton': {'type': 'boolean'},
			'eats_vegetation': {'type': 'boolean'},
			'grazer': {'type': 'boolean'},
			'hunter': {'type': 'boolean'},
			'scavenger': {'type': 'boolean'},
			'lives_in_arctic': {'type': 'boolean'},
			'lives_on_coast': {'type': 'boolean'},
			'lives_on_plains': {'type': 'boolean'},
			'lives_in_forest': {'type': 'boolean'},
			'lives_in_jungle': {'type': 'boolean'},
			'lives_in_ocean': {'type': 'boolean'},
			'lives_on_ground': {'type': 'boolean'},
			'lives_in_water': {'type': 'boolean'},
			'lives_in_trees': {'type': 'boolean'},
			'herd': {'type': 'boolean'},
			'family': {'type': 'categorical'},
			'order': {'type': 'categorical'},
			'exotic': {'type': 'categorical'}
		}

	def test_run_mammals_exotic_as_categorical():
		schema = self.schema
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		analysis.run()
	
	def test_run_mammals_exotic_as_real():
		schema = self.schema
		schema["exotic"]["type"] = "real"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		analysis.run()

	def test_run_mammals_exotic_as_count():
		schema = self.schema
		schema["exotic"]["type"] = "count"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		analysis.run()
