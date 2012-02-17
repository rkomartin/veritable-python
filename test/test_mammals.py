#! usr/bin/python

import simplejson as json
import time
import veritable
import os
from veritable.api import *
from nose.tools import raises

TEST_API_KEY = os.getenv("VERITABLE_API_KEY") or "test"
TEST_BASE_URL = os.getenv("VERITABLE_BASE_URL") or "https://api.priorknowledge.com"
DATA_FILE = "mammals.json"

data = json.load(open(DATA_FILE, 'r'))

def wait_for_analysis(a):
    while a.status() == "running":
        time.sleep(2)

class TestMammals:
	def setup(self):
		self.API = veritable.connect(TEST_API_KEY, TEST_BASE_URL)
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
		self.predictions_spec_1 = {'big': True, 'small': None}
		self.predictions_spec_2 = {"furry": True, "hairless": False, "big": False, 
			"small": False, "flippers": False, "hands": False,
			"hooves": False, "paws": True, "longneck": False, "tail": True,
			"filter_feeder": False, "horns": False, "claws": True,
			"tusks": False, "flys": None, "tunnels": False, "walks": True,
			"nocturnal": False, "eats_fish": False, "eats_meat": True,
			"eats_plankton": False, "eats_vegetation": True,
			"grazer": False, "hunter": True, "scavenger": False,
			"lives_in_arctic": False, "lives_on_coast": False,
			"lives_on_plains": False, "lives_in_forest": False,
			"lives_in_jungle": False, "lives_in_ocean": False,
			"lives_on_ground": True, "lives_in_water": False,
			"lives_in_trees": False, "herd": False, "family": "canidae",
			"order": "carnivora", "exotic": None}

	@raises
	def test_run_mammals_exotic_as_categorical(self):
		schema = self.schema
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)
	
	def test_run_mammals_exotic_as_real(self):
		schema = self.schema
		schema["exotic"]["type"] = "real"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)

	def test_run_mammals_exotic_as_count(self):
		schema = self.schema
		schema["exotic"]["type"] = "count"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)

	@raises
	def test_predict_mammals_exotic_as_categorical(self):
		schema = self.schema
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)
		analysis.predict(self.predictions_spec_1, 10)
		analysis.predict(self.predictions_spec_2, 10)
	
	def test_predict_mammals_exotic_as_real(self):
		schema = self.schema
		schema["exotic"]["type"] = "real"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)
		analysis.predict(self.predictions_spec_1, 10)
		analysis.predict(self.predictions_spec_2, 10)

	def test_predict_mammals_exotic_as_count(self):
		schema = self.schema
		schema["exotic"]["type"] = "count"
		validate_schema(schema)
		analysis = self.mammals.create_analysis(schema, description="""Full
			mammals analysis, exotic coded as categorical""",
			analysis_id="mammals_1")
		wait_for_analysis(analysis)
		analysis.predict(self.predictions_spec_1, 10)
		analysis.predict(self.predictions_spec_2, 10)
