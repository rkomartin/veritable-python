from veritable.api import *
import simplejson as json
import time

TEST_API_KEY = "test"
TEST_BASE_URL = "http://127.0.0.1:5000"
DATA_FILE = "mammals.json"

data = json.load(open(DATA_FILE, 'r'))

API = veritable_connect(TEST_API_KEY, TEST_BASE_URL)

def wait_for_analysis(analysis, poll=10):
	while analysis.status() is "pending":
		time.sleep(poll)
	if analysis.status() is "failed":
		raise Exception(analysis.get_state()["error"])

def test_end_to_end():
	# Create the table
	mammals = API.create_table("mammals", force=True)
	mammals.add_rows(data)
	schema_1 = {
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
	validate_schema(schema_1)
	analysis_1 = mammals.create_analysis(schema_1, description="""Full mammals
	  analysis, exotic coded categorical""", analysis_id="mammals_1")
	analysis_1.run()

	schema_2 = schema_1
	schema_2["exotic"]["type"] = "real"
	validate_schema(schema_2)
	analysis_2 = mammals.create_analysis(schema_2, description="""Full mammals
	  analysis, exotic coded real""", analysis_id="mammals_2")
	analysis_2.run()

	wait_for_analysis(analysis_1)
	request_1 = {'data': {'furry': True, 'small': True, 'walks': False,
				 'flys': 'null', 'exotic': 'null'}, 'count': 10}
	analysis_1.predict(request_1)

	wait_for_analysis(analysis_2)
	request_2 = {'data': {'claws': True, 'herd': False, 'eats_meat': True,
				 'exotic': 'null'}, 'count': 10}
